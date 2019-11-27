package com.alibaba.alink.operator.batch.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.SoftmaxObjFunc;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.OptimMethod;
import com.alibaba.alink.operator.common.optim.OptimizerFactory;
import com.alibaba.alink.operator.common.optim.Owlqn;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SparseVectorSummary;
import com.alibaba.alink.params.classification.SoftmaxTrainParams;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Softmax is a classifier for multi-class problem.
 *
 */
public final class SoftmaxTrainBatchOp extends BatchOperator<SoftmaxTrainBatchOp>
    implements SoftmaxTrainParams<SoftmaxTrainBatchOp> {

    public SoftmaxTrainBatchOp() {
        this(null);
    }

    public SoftmaxTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public SoftmaxTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String modelName = "softmax";

        /**
         * get parameters
         */
        boolean hasInterceptItem = getWithIntercept();
        final boolean standardization = getParams().get(LinearTrainParams.STANDARDIZATION);
        String[] featureColNames = getFeatureCols();
        String labelName = getLabelCol();
        String weightColName = getWeightCol();
        TypeInformation<?> labelType = null;
        String vectorColName = getVectorCol();
        TableSchema dataSchema = in.getSchema();
        if (null == featureColNames && null == vectorColName) {
            featureColNames = TableUtil.getNumericCols(in.getSchema(), new String[] {labelName});
            this.getParams().set(SoftmaxTrainParams.FEATURE_COLS, featureColNames);
        }

        if (null == labelType) {
            labelType = in.getColTypes()[TableUtil.findColIndex(dataSchema.getFieldNames(), labelName)];
        }

        DataSet<Row> labelIds = in
            .select(labelName)
            .distinct()
            .getDataSet().reduceGroup(new GroupReduceFunction<Row, Row>() {
                @Override
                public void reduce(Iterable<Row> values, Collector<Row> out) throws Exception {
                    List<Row> rows = new ArrayList<>();
                    for (Row row : values) {
                        rows.add(row);
                    }

                    RowComparator rowComparator = new RowComparator(0);
                    Collections.sort(rows, rowComparator);

                    for (Long i = 0L; i < rows.size(); ++i) {
                        Row ret = new Row(2);
                        ret.setField(0, rows.get(i.intValue()).getField(0));
                        ret.setField(1, i);
                        out.collect(ret);
                    }
                }
            });

        String[] keepColNames = (weightColName == null) ? new String[] {labelName}
            : new String[] {weightColName, labelName};

        // getStaticInfo produce three things: 1. transform train to vector,
        //                                     2. vector size,
        //                                     3. mean and variance of train data for standardization
        Tuple3<DataSet<Tuple2<Vector, Row>>, DataSet<Integer>, DataSet<DenseVector[]>>
            staticInfo = getStaticInfo(in, featureColNames, vectorColName, keepColNames, standardization);

        // this op will transform the data to labelVector and set labels to ids : 0, 1, 2, ...
        DataSet<Tuple3<Double, Double, Vector>> trainData = staticInfo.f0
            .mapPartition(new Transform(hasInterceptItem, standardization))
            .withBroadcastSet(staticInfo.f1, "vectorSize")
            .withBroadcastSet(labelIds, "labelIDs")
            .withBroadcastSet(staticInfo.f2, "meanVar");

        // construct a new function to do the solver opt and get a coef result. not a model.
        DataSet<Tuple2<DenseVector, double[]>> coefs
            = optimize(this.getParams(), staticInfo.f1, trainData, hasInterceptItem, labelIds);

        DataSet<Params> meta = labelIds
            .mapPartition(
                new CreateMeta(modelName, labelType, hasInterceptItem, vectorColName))
            .withBroadcastSet(staticInfo.f1, "vectorSize")
            .setParallelism(1);

        DataSet<Row> modelRows = coefs
            .mapPartition(new BuildModelFromCoefs(labelType, featureColNames, standardization))
            .withBroadcastSet(meta, "meta")
            .setParallelism(1)
            .withBroadcastSet(staticInfo.f2, "meanVar");

        this.setOutput(modelRows, new LinearModelDataConverter(labelType).getModelSchema());

        return this;
    }

    private DataSet<Tuple2<DenseVector, double[]>> optimize(Params params, DataSet<Integer> sFeatureDim,
                                                            DataSet<Tuple3<Double, Double, Vector>> trainData,
                                                            boolean hasInterceptItem,
                                                            DataSet<Row> labelIDs) {
        final double l1 = this.getParams().get(HasL1.L_1);
        final double l2 = this.getParams().get(HasL2.L_2);
        String[] featureColNames = params.get(SoftmaxTrainParams.FEATURE_COLS);
        String vectorColName = params.get(SoftmaxTrainParams.VECTOR_COL);
        if (vectorColName != null && "".equals(vectorColName)) {
            vectorColName = null;
        }
        if (featureColNames != null && featureColNames.length == 0) {
            featureColNames = null;
        }

        DataSet<Integer> numClass = labelIDs.reduceGroup(new GroupReduceFunction<Row, Integer>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Integer> out) throws Exception {
                int nClass = 0;
                for (Row row : values) {
                    nClass++;
                }
                out.collect(nClass);
            }
        });

        DataSet<Integer> coefDim;
        if (vectorColName != null && vectorColName.length() != 0) {
            coefDim = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(0)
                .map(new DimTrans(hasInterceptItem))
                .withBroadcastSet(numClass, "numClass")
                .withBroadcastSet(sFeatureDim, "vectorSize");
        } else {
            coefDim = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
                .fromElements((featureColNames.length + (hasInterceptItem ? 1 : 0))).map(
                    new RichMapFunction<Integer, Integer>() {
                        private int k1;
                        @Override
                        public void open(Configuration parameters) throws Exception {
                            super.open(parameters);
                            this.k1 = (Integer)getRuntimeContext()
                                .getBroadcastVariable("numClass").get(0) - 1;
                        }

                        @Override
                        public Integer map(Integer value) throws Exception {
                            return k1 * value;
                        }
                    }).withBroadcastSet(numClass, "numClass");
        }

        DataSet<OptimObjFunc> objFunc = numClass.reduceGroup(new GroupReduceFunction<Integer, OptimObjFunc>() {
            @Override
            public void reduce(Iterable<Integer> values, Collector<OptimObjFunc> out) throws Exception {
                int nClass = 0;
                for (Integer ele : values) {
                    nClass = ele;
                }

                Params params = new Params().set(SoftmaxTrainParams.L_1, l1)
                    .set(SoftmaxTrainParams.L_2, l2)
                    .set(ModelParamName.NUM_CLASSES, nClass);
                out.collect(new SoftmaxObjFunc(params));
            }
        });

        // solve the opt problem.
        if (params.contains("optimMethod")) {
            OptimMethod method = OptimMethod.valueOf(params.get(LinearTrainParams.OPTIM_METHOD).toUpperCase());
            return OptimizerFactory.create(objFunc, trainData, coefDim, params, method)
                .optimize();
        } else if (params.get(SoftmaxTrainParams.L_1) > 0) {
            return new Owlqn(objFunc, trainData, coefDim, params).optimize();
        } else {
            return new Lbfgs(objFunc, trainData, coefDim, params).optimize();
        }
    }

    public static class DimTrans extends AbstractRichFunction
        implements MapFunction<Integer, Integer> {
        private int k1;
        private boolean hasInterceptItem;
        private Integer featureDim = null;

        public DimTrans(boolean hasInterceptItem) {
            this.hasInterceptItem = hasInterceptItem;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.featureDim = (Integer)getRuntimeContext()
                .getBroadcastVariable("vectorSize").get(0);
            this.k1 = (Integer)getRuntimeContext()
                .getBroadcastVariable("numClass").get(0) - 1;
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            return this.k1 * (this.featureDim + (this.hasInterceptItem ? 1 : 0));
        }
    }

    /**
     * @param in              train data batch operator.
     * @param featureColNames feature column names.
     * @param vectorColName   vector column name.
     * @param keepColNames    column names kept for train process.
     * @param standardization do standardization or not.
     * @return 1. train data in vector format. 2. vector size. 3. mean and variance of train data for standardization
     */
    public static Tuple3<DataSet<Tuple2<Vector, Row>>, DataSet<Integer>, DataSet<DenseVector[]>>
    getStaticInfo(BatchOperator in, String[] featureColNames, String vectorColName,
                  String[] keepColNames, boolean standardization) {
        Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> dataSrt
            = StatisticsHelper.summaryHelper(in, featureColNames, vectorColName, keepColNames);
        DataSet<Tuple2<Vector, Row>> data = dataSrt.f0.rebalance();
        DataSet<BaseVectorSummary> srt = dataSrt.f1;
        // feature size.
        DataSet<Integer> vectorSize = srt.map(new MapFunction<BaseVectorSummary, Integer>() {
            @Override
            public Integer map(BaseVectorSummary value) throws Exception {
                return value.vectorSize();
            }
        });
        DataSet<DenseVector[]> meanVar = srt.map(new MapFunction<BaseVectorSummary, DenseVector[]>() {
            @Override
            public DenseVector[] map(BaseVectorSummary value) throws Exception {
                if (standardization) {
                    if (value instanceof SparseVectorSummary) {
                        DenseVector var = ((SparseVector)value.standardDeviation()).toDenseVector();
                        DenseVector mean = new DenseVector(var.size());
                        return new DenseVector[] {mean, var};
                    } else {
                        return new DenseVector[] {(DenseVector)value.mean(),
                            (DenseVector)value.standardDeviation()};
                    }
                } else {
                    return new DenseVector[0];
                }
            }
        });
        return Tuple3.of(data, vectorSize, meanVar);
    }

    /**
     * here, we define current labels to ids :  0, 1, 2, ...
     */
    public static class Transform extends AbstractRichFunction
        implements MapPartitionFunction<Tuple2<Vector, Row>, Tuple3<Double, Double, Vector>> {
        private boolean hasInterceptItem = true;
        private Integer vectorSize;
        private int labelSize;
        private boolean standardization;
        private HashMap<String, Double> labelMap = new HashMap<>();
        private DenseVector[] meanVar;

        public Transform(boolean hasInterceptItem, boolean standardization) {
            this.hasInterceptItem = hasInterceptItem;
            this.standardization = standardization;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.vectorSize = (Integer)getRuntimeContext()
                .getBroadcastVariable("vectorSize").get(0);

            List<Object> rows = getRuntimeContext()
                .getBroadcastVariable("labelIDs");
            this.labelSize = rows.size();

            for (int i = 0; i < this.labelSize; ++i) {
                Row row = (Row)rows.get(i);
                this.labelMap.put(row.getField(0).toString(), ((Long)row.getField(1)).doubleValue());
            }

            this.meanVar = (DenseVector[])getRuntimeContext()
                .getBroadcastVariable("meanVar").get(0);
            if (standardization) {
                for (int i = 0; i < meanVar[1].size(); ++i) {
                    if (meanVar[1].get(i) == 0) {
                        meanVar[1].set(i, 1.0);
                        meanVar[0].set(i, 0.0);
                    }
                }
            }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Vector, Row>> rows, Collector<Tuple3<Double, Double, Vector>> out)
            throws Exception {
            for (Tuple2<Vector, Row> ele : rows) {
                Double weight = (ele.f1.getArity() == 2) ? (double)ele.f1.getField(0) : 1.0;
                Vector aVector = ele.f0;
                Double val = this.labelMap.get(ele.f1.getField((ele.f1.getArity() == 2) ? 1 : 0).toString());

                if (aVector instanceof DenseVector) {
                    DenseVector bVector;
                    if (standardization) {
                        if (hasInterceptItem) {
                            bVector = new DenseVector(aVector.size() + 1);
                            bVector.set(0, 1.0);
                            for (int i = 0; i < vectorSize; ++i) {
                                bVector.set(i + 1, (aVector.get(i) - meanVar[0].get(i)) / meanVar[1].get(i));
                            }
                        } else {
                            bVector = (DenseVector)aVector;
                            for (int i = 0; i < vectorSize; ++i) {
                                bVector.set(i, aVector.get(i) / meanVar[1].get(i));
                            }
                        }
                    } else {
                        if (hasInterceptItem) {
                            bVector = new DenseVector(aVector.size() + 1);
                            bVector.set(0, 1.0);
                            for (int i = 0; i < vectorSize; ++i) {
                                bVector.set(i + 1, aVector.get(i));
                            }
                        } else {
                            bVector = (DenseVector)aVector;
                        }
                    }
                    out.collect(Tuple3.of(weight, val, bVector));

                } else {
                    SparseVector bVector = (SparseVector)aVector;

                    if (standardization) {
                        if (hasInterceptItem) {
                            bVector = bVector.prefix(1.0);
                            int[] indices = bVector.getIndices();
                            double[] vals = bVector.getValues();
                            for (int i = 1; i < indices.length; ++i) {
                                vals[i] = (vals[i] - meanVar[0].get(indices[i] - 1)) / meanVar[1].get(indices[i] - 1);
                            }
                        } else {
                            int[] indices = bVector.getIndices();
                            double[] vals = bVector.getValues();
                            for (int i = 0; i < indices.length; ++i) {
                                vals[i] = vals[i] / meanVar[1].get(indices[i]);
                            }
                        }
                    } else {
                        if (hasInterceptItem) {
                            bVector.setSize(vectorSize);
                            bVector = bVector.prefix(1.0);
                        }
                    }
                    out.collect(Tuple3.of(weight, val, bVector));
                }
            }
        }
    }

    public static class CreateMeta extends RichMapPartitionFunction<Row, Params> {
        private String modelName;
        private TypeInformation<?> labelType;
        private boolean hasInterceptItem;
        private String vectorColName;

        private CreateMeta(String modelName, TypeInformation<?> labelType,
                           boolean hasInterceptItem, String vectorColName) {
            this.modelName = modelName;
            this.labelType = labelType;
            this.hasInterceptItem = hasInterceptItem;
            this.vectorColName = vectorColName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<Params> metas) throws Exception {

            List<Row> rowList = new ArrayList<>();
            for (Row row : rows) {
                rowList.add(row);
            }
            Object[] labels = new String[rowList.size()];
            for (Row row : rowList) {
                labels[((Long)row.getField(1)).intValue()] = row.getField(0).toString();
            }

            Params meta = new Params();
            meta.set(ModelParamName.MODEL_NAME, this.modelName);
            meta.set(ModelParamName.LABEL_VALUES, labels);
            meta.set(ModelParamName.HAS_INTERCEPT_ITEM, this.hasInterceptItem);
            meta.set(ModelParamName.VECTOR_COL_NAME, vectorColName);
            meta.set(ModelParamName.NUM_CLASSES, rowList.size());
            metas.collect(meta);
        }
    }

    public class BuildModelFromCoefs extends AbstractRichFunction implements
        MapPartitionFunction<Tuple2<DenseVector, double[]>, Row> {
        private String[] featureNames;
        private Params meta;
        private int labelSize;
        private TypeInformation labelType;
        private boolean standardization;
        private DenseVector[] meanVar;

        private BuildModelFromCoefs(TypeInformation labelType, String[] featureNames, boolean standardization) {
            this.featureNames = featureNames;
            this.labelType = labelType;
            this.standardization = standardization;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.meta = (Params)getRuntimeContext()
                .getBroadcastVariable("meta").get(0);
            this.meanVar = (DenseVector[])getRuntimeContext()
                .getBroadcastVariable("meanVar").get(0);
            int iter = 0;
            this.labelSize = this.meta.get(ModelParamName.NUM_CLASSES);
            if (standardization) {
                for (int i = 0; i < meanVar[1].size(); ++i) {
                    if (meanVar[1].get(i) == 0) {
                        meanVar[1].set(i, 1.0);
                        meanVar[0].set(i, 0.0);
                    }
                }
            }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<DenseVector, double[]>> iterable,
                                 Collector<Row> collector) throws Exception {
            List<DenseVector> coefVectors = new ArrayList<>();
            boolean hasIntercept = this.meta.get(ModelParamName.HAS_INTERCEPT_ITEM);
            for (Tuple2<DenseVector, double[]> coefVector : iterable) {
                this.meta.set(ModelParamName.VECTOR_SIZE, coefVector.f0.size() / (labelSize - 1)
                    - (hasIntercept ? 1 : 0));
                this.meta.set(ModelParamName.LOSS_CURVE, coefVector.f1);
                if (standardization) {
                    if (hasIntercept) {
                        int vecSize = meanVar[0].size() + 1;
                        for (int i = 0; i < labelSize - 1; ++i) {
                            double sum = 0.0;
                            for (int j = 0; j < vecSize - 1; ++j) {
                                int idx = i * vecSize + j + 1;

                                sum += coefVector.f0.get(idx) * meanVar[0].get(j) / meanVar[1].get(j);

                                coefVector.f0.set(idx, coefVector.f0.get(idx) / meanVar[1].get(j));
                            }
                            coefVector.f0.set(i * vecSize, coefVector.f0.get(i * vecSize) - sum);
                        }
                    } else {
                        for (int i = 0; i < coefVector.f0.size(); ++i) {
                            int idx = i % meanVar[1].size();
                            coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(idx));
                        }
                    }
                }

                coefVectors.add(coefVector.f0);
            }

            LinearModelData modelData = new LinearModelData(labelType, meta, featureNames, coefVectors.get(0));
            //modelData.featureNames = featureNames;
            //modelData.vectorSize
            //    = meta.contains(ModelParamName.VECTOR_SIZE) ? meta.get(ModelParamName.VECTOR_SIZE) : 0;
            //modelData.vectorColName = meta.get(HasVectorCol.VECTOR_COL);
            //
            //modelData.coefVector = coefVectors.get(0);
            //modelData.modelName = meta.get(ModelParamName.MODEL_NAME);
            //modelData.labelValues
            //    = FeatureLabelUtil.recoverLabelType(meta.get(ModelParamName.LABEL_VALUES), labelType);
            ////modelData.linearModelType = meta.get(ModelParamName.LINEAR_MODEL_TYPE);
            //modelData.hasInterceptItem = meta.get(ModelParamName.HAS_INTERCEPT_ITEM);
            new LinearModelDataConverter(this.labelType).save(modelData, collector);
        }
    }
}
