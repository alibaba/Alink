package com.alibaba.alink.operator.batch.classification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelTrainInfo;
import com.alibaba.alink.operator.common.linear.SoftmaxModelInfo;
import com.alibaba.alink.operator.common.linear.SoftmaxModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.SoftmaxObjFunc;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.OptimizerFactory;
import com.alibaba.alink.operator.common.optim.Owlqn;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.params.classification.SoftmaxTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

/**
 * Softmax is a classifier for multi-class problem.
 */
public final class SoftmaxTrainBatchOp extends BatchOperator<SoftmaxTrainBatchOp>
    implements SoftmaxTrainParams<SoftmaxTrainBatchOp>, WithTrainInfo<LinearModelTrainInfo, SoftmaxTrainBatchOp>,
    WithModelInfoBatchOp<SoftmaxModelInfo, SoftmaxTrainBatchOp, SoftmaxModelInfoBatchOp> {
    private static final long serialVersionUID = 2291776467437931890L;

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
        TypeInformation<?> labelType = null;
        String vectorColName = getVectorCol();
        TableSchema dataSchema = in.getSchema();
        if (null == featureColNames && null == vectorColName) {
            featureColNames = TableUtil.getNumericCols(in.getSchema(), new String[] {labelName});
            this.getParams().set(SoftmaxTrainParams.FEATURE_COLS, featureColNames);
        }

        if (null == labelType) {
            labelType = TableUtil.findColTypeWithAssertAndHint(dataSchema, labelName);
        }

        DataSet<Tuple3<Double, Object, Vector>>
            initData = BaseLinearModelTrainBatchOp.transform(in, getParams(), false, standardization);

        DataSet<Tuple3<DenseVector[], Object[], Integer>>
            utilInfo = BaseLinearModelTrainBatchOp.getUtilInfo(initData, standardization, false);

        DataSet<Row> labelIds = utilInfo.flatMap(new FlatMapFunction<Tuple3<DenseVector[], Object[], Integer>, Row>() {
            private static final long serialVersionUID = 6773656778135257500L;

            @Override
            public void flatMap(Tuple3<DenseVector[], Object[], Integer> value, Collector<Row> out) throws Exception {
                List<Row> rows = new ArrayList<>();
                for (Object obj : value.f1) {
                    rows.add(Row.of(obj));
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

        DataSet<DenseVector[]> meanVar = utilInfo.map(
            new MapFunction<Tuple3<DenseVector[], Object[], Integer>, DenseVector[]>() {
                private static final long serialVersionUID = 2633660310293456071L;

                @Override
                public DenseVector[] map(Tuple3<DenseVector[], Object[], Integer> value)
                    throws Exception {
                    return value.f0;
                }
            });

        DataSet<Integer> featSize = utilInfo.map(
            new MapFunction<Tuple3<DenseVector[], Object[], Integer>, Integer>() {
                private static final long serialVersionUID = -8902907232968104891L;

                @Override
                public Integer map(Tuple3<DenseVector[], Object[], Integer> value)
                    throws Exception {
                    return value.f2;
                }
            });

        // this op will transform the data to labelVector and set labels to ids : 0, 1, 2, ...
        DataSet<Tuple3<Double, Double, Vector>> trainData = initData
            .mapPartition(new PreProcess(hasInterceptItem, standardization))
            .withBroadcastSet(labelIds, "labelIDs")
            .withBroadcastSet(meanVar, "meanVar");

        // construct a new function to do the solver opt and get a coef result. not a model.
        DataSet<Tuple2<DenseVector, double[]>> coefs
            = optimize(this.getParams(), featSize, trainData, hasInterceptItem, labelIds);

        DataSet<Params> meta = labelIds
            .mapPartition(
                new CreateMeta(modelName, hasInterceptItem, vectorColName))
            .setParallelism(1);

        DataSet<Row> modelRows = coefs
            .mapPartition(new BuildModelFromCoefs(labelType, featureColNames, standardization))
            .withBroadcastSet(meta, "meta")
            .setParallelism(1)
            .withBroadcastSet(meanVar, "meanVar");

        this.setOutput(modelRows, new LinearModelDataConverter(labelType).getModelSchema());

        this.setSideOutputTables(getSideTablesOfCoefficient(modelRows));
        return this;
    }

    private DataSet<Tuple2<DenseVector, double[]>> optimize(Params params, DataSet<Integer> sFeatureDim,
                                                            DataSet<Tuple3<Double, Double, Vector>> trainData,
                                                            boolean hasInterceptItem,
                                                            DataSet<Row> labelIDs) {
        final double l1 = getL1();
        final double l2 = getL2();
        String[] featureColNames = params.get(SoftmaxTrainParams.FEATURE_COLS);
        String vectorColName = params.get(SoftmaxTrainParams.VECTOR_COL);

        DataSet<Integer> numClass = labelIDs.reduceGroup(new GroupReduceFunction<Row, Integer>() {
            private static final long serialVersionUID = -8665284351311032858L;

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
            coefDim = sFeatureDim.map(new RichMapFunction<Integer, Integer>() {
                private static final long serialVersionUID = 3041217732252202526L;
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
        } else {
            coefDim = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
                .fromElements((featureColNames.length + (hasInterceptItem ? 1 : 0))).map(
                    new RichMapFunction<Integer, Integer>() {
                        private static final long serialVersionUID = 3133807849008897754L;
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
            private static final long serialVersionUID = -4647154716237314079L;

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
        if (params.contains(LinearTrainParams.OPTIM_METHOD)) {
            OptimMethod method = params.get(LinearTrainParams.OPTIM_METHOD);
            return OptimizerFactory.create(objFunc, trainData, coefDim, params, method)
                .optimize();
        } else if (params.get(SoftmaxTrainParams.L_1) > 0) {
            return new Owlqn(objFunc, trainData, coefDim, params).optimize();
        } else {
            return new Lbfgs(objFunc, trainData, coefDim, params).optimize();
        }
    }

    /**
     * here, we define current labels to ids :  0, 1, 2, ...
     */
    public static class PreProcess extends AbstractRichFunction
        implements MapPartitionFunction<Tuple3<Double, Object, Vector>, Tuple3<Double, Double, Vector>> {
        private static final long serialVersionUID = -5610968130256583178L;
        private boolean hasInterceptItem = true;
        private int labelSize;
        private boolean standardization;
        private HashMap<String, Double> labelMap = new HashMap<>();
        private DenseVector[] meanVar;

        public PreProcess(boolean hasInterceptItem, boolean standardization) {
            this.hasInterceptItem = hasInterceptItem;
            this.standardization = standardization;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Object> rows = getRuntimeContext()
                .getBroadcastVariable("labelIDs");
            this.labelSize = rows.size();

            for (int i = 0; i < this.labelSize; ++i) {
                Row row = (Row)rows.get(i);
                this.labelMap.put(row.getField(0).toString(), ((Long)row.getField(1)).doubleValue());
            }

            this.meanVar = (DenseVector[])getRuntimeContext()
                .getBroadcastVariable("meanVar").get(0);
        }

        @Override
        public void mapPartition(Iterable<Tuple3<Double, Object, Vector>> rows,
                                 Collector<Tuple3<Double, Double, Vector>> out)
            throws Exception {
            for (Tuple3<Double, Object, Vector> ele : rows) {
                Double weight = ele.f0;
                Vector aVector = ele.f2;
                Double val = this.labelMap.get(ele.f1.toString());
                if (ele.f0 > 0) {
                    if (aVector instanceof DenseVector) {
                        if (standardization) {
                            if (hasInterceptItem) {
                                for (int i = 0; i < aVector.size(); ++i) {
                                    aVector.set(i, (aVector.get(i) - meanVar[0].get(i)) / meanVar[1].get(i));
                                }
                            } else {
                                for (int i = 0; i < aVector.size(); ++i) {
                                    aVector.set(i, aVector.get(i) / meanVar[1].get(i));
                                }
                            }
                        }
                    } else {
                        if (standardization) {
                            for (int i = 0; i < aVector.size(); ++i) {
                                aVector.set(i, (aVector.get(i) - meanVar[0].get(i)) / meanVar[1].get(i));
                            }
                        }
                    }
                    out.collect(Tuple3.of(weight, val, aVector));
                }
            }
        }
    }

    public static class CreateMeta implements MapPartitionFunction<Row, Params> {
        private static final long serialVersionUID = 8430372703655142394L;
        private String modelName;
        private boolean hasInterceptItem;
        private String vectorColName;

        private CreateMeta(String modelName, boolean hasInterceptItem, String vectorColName) {
            this.modelName = modelName;
            this.hasInterceptItem = hasInterceptItem;
            this.vectorColName = vectorColName;
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
        private static final long serialVersionUID = -5211654314835044657L;
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
            this.labelSize = this.meta.get(ModelParamName.NUM_CLASSES);
        }

        @Override
        public void mapPartition(Iterable<Tuple2<DenseVector, double[]>> iterable,
                                 Collector<Row> collector) throws Exception {
            List<DenseVector> coefVectors = new ArrayList<>();
            boolean hasIntercept = this.meta.get(ModelParamName.HAS_INTERCEPT_ITEM);
            double[] convInfo = null;
            for (Tuple2<DenseVector, double[]> coefVector : iterable) {
                convInfo = coefVector.f1;
                this.meta.set(ModelParamName.VECTOR_SIZE, coefVector.f0.size() / (labelSize - 1)
                    - (hasIntercept ? 1 : 0));
                this.meta.set(ModelParamName.LOSS_CURVE, coefVector.f1);
                if (standardization) {
                    if (hasIntercept) {
                        int vecSize = meanVar[0].size();
                        for (int i = 0; i < labelSize - 1; ++i) {
                            double sum = 0.0;
                            for (int j = 1; j < vecSize; ++j) {
                                int idx = i * vecSize + j;

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
            modelData.convergenceInfo = convInfo;
            new LinearModelDataConverter(this.labelType).save(modelData, collector);
        }
    }

    private Table[] getSideTablesOfCoefficient(DataSet<Row> modelRow) {
        DataSet<LinearModelData> model = modelRow.mapPartition(new MapPartitionFunction<Row, LinearModelData>() {
            private static final long serialVersionUID = 2063366042018382802L;

            @Override
            public void mapPartition(Iterable<Row> values, Collector<LinearModelData> out) throws Exception {
                List<Row> rows = new ArrayList<>();
                for (Row row : values) {
                    rows.add(row);
                }
                out.collect(new LinearModelDataConverter().load(rows));
            }
        }).setParallelism(1);

        DataSet<Row> summary = model
            .mapPartition(
                new RichMapPartitionFunction<LinearModelData, Row>() {
                    private static final long serialVersionUID = 8785824618242390100L;

                    @Override
                    public void mapPartition(Iterable<LinearModelData> values, Collector<Row> out) throws Exception {

                    LinearModelData model = values.iterator().next();
                    double[] cinfo = model.convergenceInfo;
                    out.collect(Row.of(0, JsonConverter.toJson(model.getMetaInfo())));
                    out.collect(Row.of(4, JsonConverter.toJson(cinfo)));

                }
            }).setParallelism(1).withBroadcastSet(model, "model");

        Table summaryTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), summary, new TableSchema(
            new String[] {"title", "info"}, new TypeInformation[] {Types.INT, Types.STRING}));

        return new Table[] {summaryTable};
    }

    @Override
    public SoftmaxModelInfoBatchOp getModelInfoBatchOp() {
        return new SoftmaxModelInfoBatchOp().linkFrom(this);
    }

    @Override
    public LinearModelTrainInfo createTrainInfo(List<Row> rows) {
        return new LinearModelTrainInfo(rows);
    }

    @Override
    public BatchOperator<?> getSideOutputTrainInfo() {
        return this.getSideOutput(0);
    }

}

