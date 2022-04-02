package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Base FM model training.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
    @PortSpec(PortType.MODEL),
    @PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_INFO)
})

@ParamSelectColumnSpec(name = "featureCols",
    allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
    allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
    allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule

public abstract class BaseFmTrainBatchOp<T extends BaseFmTrainBatchOp<T>> extends BatchOperator<T> {

    public static final String LABEL_VALUES = "labelValues";
    public static final String VEC_SIZE = "vecSize";
    private static final long serialVersionUID = -5308557491809175331L;
    protected TypeInformation<?> labelType;

    /**
     * @param params parameters needed by training process.
     */
    public BaseFmTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * construct function.
     */
    public BaseFmTrainBatchOp() {
        super(new Params());
    }

    /**
     * The api for optimizer.
     *
     * @param trainData training Data.
     * @param vecSize   vector size.
     * @param params    parameters.
     * @param dim       dimension.
     * @return model coefficient.
     */
    protected abstract DataSet<Tuple2<FmDataFormat, double[]>>
    optimize(DataSet<Tuple3<Double, Double, Vector>> trainData,
             DataSet<Integer> vecSize,
             final Params params,
             final int[] dim);

    /**
     * The api for transforming model format.
     *
     * @param model       model with fm data format.
     * @param labelValues label values.
     * @param vecSize     vector size.
     * @param params      parameters.
     * @param dim         dimension.
     * @param isRegProc   is regression process.
     * @param labelType   label type.
     * @return model rows.
     */
    protected abstract DataSet<Row> transformModel(
            DataSet<Tuple2<FmDataFormat, double[]>> model,
            DataSet<Object[]> labelValues,
            DataSet<Integer> vecSize,
            final Params params,
            final int[] dim,
            boolean isRegProc,
            TypeInformation<?> labelType);

    /**
     * do the operation of this op.
     *
     * @param inputs the linked inputs
     * @return this class.
     */
    @Override
    public T linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        // Get parameters of this algorithm.
        Params params = getParams();
        if (params.contains(HasFeatureCols.FEATURE_COLS) && params.contains(HasVectorCol.VECTOR_COL)) {
            throw new RuntimeException("featureCols and vectorCol cannot be set at the same time.");
        }
        int[] dim = new int[3];
        dim[0] = params.get(FmTrainParams.WITH_INTERCEPT) ? 1 : 0;
        dim[1] = params.get(FmTrainParams.WITH_LINEAR_ITEM) ? 1 : 0;
        dim[2] = params.get(FmTrainParams.NUM_FACTOR);

        boolean isRegProc = params.get(ModelParamName.TASK).equals(Task.REGRESSION);
        this.labelType = isRegProc ? Types.DOUBLE : in.getColTypes()[TableUtil
                .findColIndex(in.getColNames(), params.get(FmTrainParams.LABEL_COL))];

        // Transform data to Tuple3 format <weight, label, feature vector>.
        DataSet<Tuple3<Double, Object, Vector>> initData = transform(in, params, isRegProc);

        // Get some util info, such as featureSize and labelValues.
        DataSet<Tuple2<Object[], Integer>> utilInfo = getUtilInfo(initData, isRegProc);
        DataSet<Integer> featSize = utilInfo.map(
                new MapFunction<Tuple2<Object[], Integer>, Integer>() {
                    private static final long serialVersionUID = 1099531852518545431L;

                    @Override
                    public Integer map(Tuple2<Object[], Integer> value) {
                        return value.f1;
                    }
                });
        DataSet<Object[]> labelValues = utilInfo.flatMap(
                new FlatMapFunction<Tuple2<Object[], Integer>, Object[]>() {
                    private static final long serialVersionUID = -4407775357759305675L;

                    @Override
                    public void flatMap(Tuple2<Object[], Integer> value,
                                        Collector<Object[]> out) {
                        out.collect(value.f0);
                    }
                });

        DataSet<Tuple3<Double, Double, Vector>>
                trainData = transferLabel(initData, isRegProc, labelValues);

        DataSet<Tuple2<FmDataFormat, double[]>> model = optimize(trainData, featSize, params, dim);

        DataSet<Row> modelRows = transformModel(model, labelValues, featSize, params, dim, isRegProc, labelType);

        this.setOutput(modelRows, new FmModelDataConverter(labelType).getModelSchema());
        this.setSideOutputTables(getSideTablesOfCoefficient(modelRows, labelType));
        return (T) this;
    }

    /**
     * Do transform for train data.
     *
     * @param initData    initial data.
     * @param isRegProc   train process is regression or classification.
     * @param labelValues label values.
     * @return data for fm training.
     */
    private static DataSet<Tuple3<Double, Double, Vector>> transferLabel(
            DataSet<Tuple3<Double, Object, Vector>> initData,
            final boolean isRegProc,
            DataSet<Object[]> labelValues) {
        return initData.mapPartition(
                new RichMapPartitionFunction<Tuple3<Double, Object, Vector>, Tuple3<Double, Double, Vector>>() {
                    private static final long serialVersionUID = 1609901151679856341L;
                    private Object[] labelValues = null;

                    @Override
                    public void open(Configuration parameters) {
                        this.labelValues = (Object[]) getRuntimeContext()
                                .getBroadcastVariable(LABEL_VALUES).get(0);
                    }

                    @Override
                    public void mapPartition(Iterable<Tuple3<Double, Object, Vector>> values,
                                             Collector<Tuple3<Double, Double, Vector>> out) {
                        for (Tuple3<Double, Object, Vector> value : values) {

                            if (value.f0 > 0) {
                                Double label = isRegProc ? Double.parseDouble(value.f1.toString())
                                        : (value.f1.equals(labelValues[0]) ? 1.0 : 0.0);
                                out.collect(Tuple3.of(value.f0, label, value.f2));
                            }
                        }
                    }
                })
                .withBroadcastSet(labelValues, LABEL_VALUES);
    }

    /**
     * @param initData  get some useful info from initial data.
     * @param isRegProc train process is regression or classification.
     * @return useful data, including label values and vector size.
     */
    private static DataSet<Tuple2<Object[], Integer>> getUtilInfo(
            DataSet<Tuple3<Double, Object, Vector>> initData,
            boolean isRegProc) {
        return initData.filter(
                new FilterFunction<Tuple3<Double, Object, Vector>>() {
                    private static final long serialVersionUID = 4954837288144406855L;

                    @Override
                    public boolean filter(Tuple3<Double, Object, Vector> value) {
                        return value.f0 < 0.0;
                    }
                }).reduceGroup(
                new GroupReduceFunction<Tuple3<Double, Object, Vector>, Tuple2<Object[],
                        Integer>>() {
                    private static final long serialVersionUID = 3520762756658301627L;

                    @Override
                    public void reduce(Iterable<Tuple3<Double, Object, Vector>> values,
                                       Collector<Tuple2<Object[], Integer>> out) {
                        int size = -1;
                        Set<Object> labelValues = new HashSet<>();
                        for (Tuple3<Double, Object, Vector> value : values) {
                            Tuple2<Integer, Object[]>
                                    labelVals = (Tuple2<Integer, Object[]>) value.f1;
                            Collections.addAll(labelValues, labelVals.f1);
                            size = Math.max(size, labelVals.f0);

                        }
                        Object[] labelsSort = isRegProc ? labelValues.toArray() : orderLabels(labelValues);
                        out.collect(Tuple2.of(labelsSort, size));
                    }
                });
    }

    /**
     * order by the dictionary order, only classification problem need do this process.
     *
     * @param unorderedLabelRows unordered label rows
     * @return Ordered labels.
     */
    protected static Object[] orderLabels(Iterable<Object> unorderedLabelRows) {
        List<Object> tmpArr = new ArrayList<>();
        for (Object row : unorderedLabelRows) {
            tmpArr.add(row);
        }
        Object[] labels = tmpArr.toArray(new Object[0]);
        Preconditions.checkState((labels.length == 2), "labels count should be 2 in 2 classification algo.");
        if (labels[0] instanceof Number) {
            if (((Number) labels[0]).doubleValue() + ((Number) labels[1]).doubleValue() == 1.0) {
                if (((Number) labels[0]).doubleValue() == 0.0) {
                    Object t = labels[0];
                    labels[0] = labels[1];
                    labels[1] = t;
                }
            }
        } else {
            String str0 = labels[0].toString();
            String str1 = labels[1].toString();
            String positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;

            if (labels[1].toString().equals(positiveLabelValueString)) {
                Object t = labels[0];
                labels[0] = labels[1];
                labels[1] = t;
            }
        }
        return labels;
    }

    /**
     * Transform train data to Tuple3 format.
     *
     * @param in     train data in row format.
     * @param params train parameters.
     * @return Tuple3 format train data <weight, label, vector></>.
     */
    private DataSet<Tuple3<Double, Object, Vector>> transform(BatchOperator<?> in,
                                                              Params params,
                                                              boolean isRegProc) {
        String[] featureColNames = params.get(FmTrainParams.FEATURE_COLS);
        String labelName = params.get(FmTrainParams.LABEL_COL);
        String weightColName = params.get(FmTrainParams.WEIGHT_COL);
        String vectorColName = params.get(FmTrainParams.VECTOR_COL);
        TableSchema dataSchema = in.getSchema();
        if (null == featureColNames && null == vectorColName) {
            featureColNames = TableUtil.getNumericCols(dataSchema, new String[]{labelName});
            params.set(FmTrainParams.FEATURE_COLS, featureColNames);
        }
        int[] featureIndices = null;
        int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
        if (featureColNames != null) {
            featureIndices = new int[featureColNames.length];
            for (int i = 0; i < featureColNames.length; ++i) {
                int idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), featureColNames[i]);
                featureIndices[i] = idx;
            }
        }
        int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
                weightColName)
                : -1;
        int vecIdx = vectorColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName)
                : -1;

        return in.getDataSet().mapPartition(new Transform(isRegProc, weightIdx,
                vecIdx, featureIndices, labelIdx));
    }

    /**
     * Transform the train data to Tuple3 format: Tuple3<weightValue, labelValue, featureSparseVector>
     */
    private static class Transform extends RichMapPartitionFunction<Row, Tuple3<Double, Object, Vector>> {
        private static final long serialVersionUID = 5935792357245627952L;
        private final int vecIdx;
        private final int labelIdx;
        private final int weightIdx;
        private final boolean isRegProc;
        private final int[] featureIndices;

        public Transform(boolean isRegProc, int weightIndx, int vecIdx, int[] featureIndices, int labelIdx) {
            this.vecIdx = vecIdx;
            this.labelIdx = labelIdx;
            this.weightIdx = weightIndx;
            this.isRegProc = isRegProc;
            this.featureIndices = featureIndices;
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Tuple3<Double, Object, Vector>> out)
                throws Exception {
            Set<Object> labelValues = new HashSet<>();
            int size = -1;
            if (featureIndices != null) {
                size = featureIndices.length;
            }
            for (Row row : values) {
                Double weight = weightIdx == -1 ? 1.0 : ((Number) row.getField(weightIdx)).doubleValue();
                Object label = row.getField(labelIdx);

                if (!this.isRegProc) {
                    labelValues.add(label);
                } else {
                    labelValues.add(0.0);
                }

                Vector vec;
                if (featureIndices != null) {
                    vec = new DenseVector(featureIndices.length);
                    for (int i = 0; i < featureIndices.length; ++i) {
                        vec.set(i, ((Number) row.getField(featureIndices[i])).doubleValue());
                    }
                } else {
                    vec = VectorUtil.getVector(row.getField(vecIdx));
                    if (vec instanceof SparseVector) {
                        int[] indices = ((SparseVector) vec).getIndices();
                        for (int index : indices) {
                            size = (vec.size() > 0) ? vec.size() : Math.max(size, index + 1);
                        }
                    } else {
                        size = ((DenseVector) vec).getData().length;
                    }
                }
                out.collect(Tuple3.of(weight, label, vec));
            }
            out.collect(
                    Tuple3.of(-1.0, Tuple2.of(size, labelValues.toArray()), new DenseVector(0)));
        }

    }

    private Table[] getSideTablesOfCoefficient(DataSet<Row> modelRow, final TypeInformation<?> labelType) {
        DataSet<FmModelData> model = modelRow.mapPartition(new MapPartitionFunction<Row, FmModelData>() {
            private static final long serialVersionUID = 2063366042018382802L;

            @Override
            public void mapPartition(Iterable<Row> values, Collector<FmModelData> out) {
                List<Row> rows = new ArrayList<>();
                for (Row row : values) {
                    rows.add(row);
                }
                out.collect(new FmModelDataConverter(labelType).load(rows));
            }
        }).setParallelism(1);

        DataSet<Row> summary = model
                .mapPartition(
                        new RichMapPartitionFunction<FmModelData, Row>() {
                            private static final long serialVersionUID = 8785824618242390100L;

                            @Override
                            public void mapPartition(Iterable<FmModelData> values, Collector<Row> out) {

                                FmModelData model = values.iterator().next();
                                double[] cinfo = model.convergenceInfo;
                                Params meta = new Params();
                                meta.set(ModelParamName.VECTOR_SIZE, model.vectorSize);
                                meta.set(ModelParamName.LABEL_VALUES, model.labelValues);
                                meta.set(FmTrainParams.WITH_LINEAR_ITEM, model.dim[1] == 1);
                                meta.set(FmTrainParams.WITH_INTERCEPT, model.dim[0] == 1);
                                meta.set(FmTrainParams.NUM_FACTOR, model.dim[2]);
                                out.collect(Row.of(0, JsonConverter.toJson(meta)));
                                out.collect(Row.of(1, JsonConverter.toJson(cinfo)));

                            }
                        }).setParallelism(1).withBroadcastSet(model, "model");

        Table summaryTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), summary, new TableSchema(
                new String[]{"title", "info"}, new TypeInformation[]{Types.INT, Types.STRING}));

        return new Table[]{summaryTable};
    }

    public enum Task {
        /**
         * regression problem.
         */
        REGRESSION,
        /**
         * binary classification problem.
         */
        BINARY_CLASSIFICATION
    }

    /**
     * loss function interface
     */
    public interface LossFunction extends Serializable {
        /**
         * calculate loss of sample.
         *
         * @param yTruth Truth label.
         * @param y Predicted label.
         * @return Loss.
         */
        double l(double yTruth, double y);

        /**
         * calculate dldy of sample
         *
         * @param yTruth Truth label.
         * @param y Predicted label.
         * @return dldy.
         */
        double dldy(double yTruth, double y);
    }

    /**
     * loss function for regression task
     */
    public static class SquareLoss implements LossFunction {
        private static final long serialVersionUID = -3903508209287601504L;
        private final double maxTarget;
        private final double minTarget;

        public SquareLoss(double maxTarget, double minTarget) {
            this.maxTarget = maxTarget;
            this.minTarget = minTarget;
        }

        @Override
        public double l(double yTruth, double y) {
            return (yTruth - y) * (yTruth - y);
        }

        @Override
        public double dldy(double yTruth, double y) {
            // a trick borrowed from libFM
            y = Math.min(y, maxTarget);
            y = Math.max(y, minTarget);

            return 2.0 * (y - yTruth);
        }
    }

    /**
     * loss function for binary classification task
     */
    public static class LogitLoss implements LossFunction {
        private static final long serialVersionUID = -166213844104644622L;

        @Override
        public double l(double yTruth, double y) { // yTruth in {0, 1}
            double logit = sigmoid(y);
            if (yTruth < 0.5) {
                return -Math.log(1. - logit);
            } else if (yTruth >= 0.5) {
                return -Math.log(logit);
            } else {
                throw new RuntimeException("Invalid label: " + yTruth);
            }
        }

        @Override
        public double dldy(double yTruth, double y) {
            return sigmoid(y) - yTruth;
        }

        private double sigmoid(double y) {
            return 1.0 / (1.0 + Math.exp(-y));
        }
    }

    /**
     * the data structure of FM model data.
     */
    public static class FmDataFormat implements Serializable {
        private static final long serialVersionUID = 192926704450234984L;
        public double[][] factors;
        public double bias;
        public int[] dim;

        // empty constructor to make it POJO
        public FmDataFormat() {
        }

        public FmDataFormat(int vecSize, int[] dim, double initStdev) {
            this.dim = dim;
            this.factors = new double[vecSize][dim[2] + dim[1]];
            reset(initStdev);
        }

        public FmDataFormat(int vecSize, int numField, int[] dim, double initStdev) {
            this.dim = dim;
            this.factors = new double[vecSize * numField][dim[2] + dim[1]];
            reset(initStdev);
        }

        public void reset(double initStdev) {
            Random rand = new Random(2020);
            for (int i = 0; i < factors.length; ++i) {
                for (int j = 0; j < factors[0].length; ++j) {
                    factors[i][j] = rand.nextGaussian() * initStdev;
                }
            }
        }
    }
}



