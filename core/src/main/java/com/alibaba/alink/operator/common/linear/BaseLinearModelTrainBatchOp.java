package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.unarylossfunc.*;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.OptimMethod;
import com.alibaba.alink.operator.common.optim.OptimizerFactory;
import com.alibaba.alink.operator.common.optim.Owlqn;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SparseVectorSummary;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of linear model training. Linear binary classification and linear regression algorithms should inherit
 * this class. Then it only need to write the code of loss function and regular item.
 *
 * @param <T> parameter of this class. Maybe the Svm, linearRegression or Lr parameter.
 */
public abstract class BaseLinearModelTrainBatchOp<T extends BaseLinearModelTrainBatchOp<T>> extends BatchOperator<T> {
    private String modelName;
    private LinearModelType linearModelType;
    private static final int NUM_FEATURE_THRESHOLD = 10000;
    private static final String META = "meta";
    private static final String MEAN_VAR = "meanVar";
    private static final String VECTOR_SIZE = "vectorSize";
    private static final String LABEL_VALUES = "labelValues";

    /**
     * @param params    parameters needed by training process.
     * @param modelType model type: LR, SVR, SVM, Ridge ...
     * @param modelName name of model.
     */
    public BaseLinearModelTrainBatchOp(Params params, LinearModelType modelType, String modelName) {
        super(params);
        this.modelName = modelName;
        this.linearModelType = modelType;
    }

    @Override
    public T linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        // Get parameters of this algorithm.
        Params params = getParams();
        // Get type of processing: regression or not
        boolean isRegProc = getIsRegProc(params, linearModelType, modelName);
        // Get label info : including label values and label type.
        Tuple2<DataSet<Object>, TypeInformation> labelInfo = getLabelInfo(in, params, isRegProc);
        // Transform data to Tuple3 format.//weight, label, feature vector.
        DataSet<Tuple3<Double, Double, Vector>> initData = transform(in, params, labelInfo.f0, isRegProc);
        // Get statistics variables : including vector size, mean and variance of train data.
        Tuple2<DataSet<Integer>, DataSet<DenseVector[]>>
            statInfo = getStatInfo(initData, params.get(LinearTrainParams.STANDARDIZATION));
        // Do standardization and interception to train data.
        DataSet<Tuple3<Double, Double, Vector>> trainData = preProcess(initData, params, statInfo.f1);
        // Solve the optimization problem.
        DataSet<Tuple2<DenseVector, double[]>> coefVectorSet = optimize(params, statInfo.f0,
            trainData, linearModelType, MLEnvironmentFactory.get(getMLEnvironmentId()));
        // Prepare the meta info of linear model.
        DataSet<Params> meta = labelInfo.f0
            .mapPartition(new CreateMeta(modelName, linearModelType, isRegProc, params))
            .setParallelism(1);
        // Build linear model rows, the format to be output.
        DataSet<Row> modelRows;
        String[] featureColTypes = getFeatureTypes(in, params.get(LinearTrainParams.FEATURE_COLS));
        modelRows = coefVectorSet
            .mapPartition(new BuildModelFromCoefs(labelInfo.f1,
                params.get(LinearTrainParams.FEATURE_COLS),
                params.get(LinearTrainParams.STANDARDIZATION),
                params.get(LinearTrainParams.WITH_INTERCEPT), featureColTypes))
            .withBroadcastSet(meta, META)
            .withBroadcastSet(statInfo.f1, MEAN_VAR)
            .setParallelism(1);
        // Convert the model rows to table.
        this.setOutput(modelRows, new LinearModelDataConverter(labelInfo.f1).getModelSchema());
        return (T)this;
    }

    /**
     * @param trainData       train data.
     * @param standardization do standardization or not.
     * @return return one element. 1. vector size. 2. mean and variance of train data for standardization
     */
    private Tuple2<DataSet<Integer>, DataSet<DenseVector[]>> getStatInfo(
        DataSet<Tuple3<Double, Double, Vector>> trainData,
        final boolean standardization) {
        if (standardization) {
            DataSet<BaseVectorSummary> summary = StatisticsHelper.summary(trainData.map(
                new MapFunction<Tuple3<Double, Double, Vector>, Vector>() {
                    @Override
                    public Vector map(Tuple3<Double, Double, Vector> value) throws Exception {
                        return value.f2;
                    }
                }).withForwardedFields());

            DataSet<Integer> coefficientDim = summary.map(new MapFunction<BaseVectorSummary, Integer>() {
                @Override
                public Integer map(BaseVectorSummary value) throws Exception {
                    return value.vectorSize();
                }
            });

            DataSet<DenseVector[]> meanVar = summary.map(new MapFunction<BaseVectorSummary, DenseVector[]>() {
                @Override
                public DenseVector[] map(BaseVectorSummary value) {
                    if (value instanceof SparseVectorSummary) {
                        // If train data format is sparse vector, use maxAbs as variance and set mean zero,
                        // then, the standardization operation will turn into a scale operation.
                        // Because if do standardization to sparse vector, vector will be convert to be a dense one.
                        DenseVector max = ((SparseVector)value.max()).toDenseVector();
                        DenseVector min = ((SparseVector)value.min()).toDenseVector();
                        for (int i = 0; i < max.size(); ++i) {
                            max.set(i, Math.max(Math.abs(max.get(i)), Math.abs(min.get(i))));
                            min.set(i, 0.0);
                        }
                        return new DenseVector[] {min, max};
                    } else {
                        return new DenseVector[] {(DenseVector)value.mean(),
                            (DenseVector)value.standardDeviation()};
                    }
                }
            });
            return Tuple2.of(coefficientDim, meanVar);
        } else {
            // If not do standardization, the we use mapReduce to get vector Dim. Mean and var set zero vector.
            DataSet<Integer> coefficientDim = trainData.mapPartition(
                new MapPartitionFunction<Tuple3<Double, Double, Vector>, Integer>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Double, Double, Vector>> values, Collector<Integer> out)
                        throws Exception {
                        int ret = -1;
                        for (Tuple3<Double, Double, Vector> val : values) {
                            if (val.f2 instanceof DenseVector) {
                                ret = ((DenseVector)val.f2).getData().length;
                                break;
                            } else {
                                int[] ids = ((SparseVector)val.f2).getIndices();
                                for (int id : ids) {
                                    ret = Math.max(ret, id + 1);
                                }
                            }
                        }
                        out.collect(ret);
                    }
                }).reduceGroup(new GroupReduceFunction<Integer, Integer>() {
                @Override
                public void reduce(Iterable<Integer> values, Collector<Integer> out) {
                    int ret = -1;
                    for (int vSize : values) {
                        ret = Math.max(ret, vSize);
                    }
                    out.collect(ret);
                }
            });

            DataSet<DenseVector[]> meanVar = coefficientDim.map(new MapFunction<Integer, DenseVector[]>() {
                @Override
                public DenseVector[] map(Integer value) {
                    return new DenseVector[] {new DenseVector(0), new DenseVector(0)};
                }
            });
            return Tuple2.of(coefficientDim, meanVar);
        }
    }

    /**
     * order by the dictionary order,
     * only classification problem need do this process.
     *
     * @param unorderedLabelRows unordered label rows
     * @return
     */
    private static Object[] orderLabels(Iterable<Object> unorderedLabelRows) {
        List<Object> tmpArr = new ArrayList<>();
        for (Object row : unorderedLabelRows) {
            tmpArr.add(row);
        }
        Object[] labels = tmpArr.toArray(new Object[0]);
        Preconditions.checkState((labels.length == 2), "labels count should be 2 in 2 classification algo.");
        String str0 = labels[0].toString();
        String str1 = labels[1].toString();
        String positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;

        if (labels[1].toString().equals(positiveLabelValueString)) {
            Object t = labels[0];
            labels[0] = labels[1];
            labels[1] = t;
        }
        return labels;
    }

    /**
     * optimize linear problem
     *
     * @param params     parameters need by optimizer.
     * @param vectorSize vector size.
     * @param trainData  train Data.
     * @param modelType  linear model type.
     * @param session    machine learning environment
     * @return coefficient of linear problem.
     */
    public static DataSet<Tuple2<DenseVector, double[]>> optimize(Params params,
                                                                  DataSet<Integer> vectorSize,
                                                                  DataSet<Tuple3<Double, Double, Vector>> trainData,
                                                                  LinearModelType modelType,
                                                                  MLEnvironment session) {
        boolean hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
        String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
        String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
        if ("".equals(vectorColName)) {
            vectorColName = null;
        }
        if (org.apache.commons.lang3.ArrayUtils.isEmpty(featureColNames)) {
            featureColNames = null;
        }

        DataSet<Integer> coefficientDim;

        if (vectorColName != null && vectorColName.length() != 0) {
            coefficientDim = session.getExecutionEnvironment().fromElements(0)
                .map(new DimTrans(hasInterceptItem, modelType))
                .withBroadcastSet(vectorSize, VECTOR_SIZE);
        } else {
            coefficientDim = session.getExecutionEnvironment().fromElements(featureColNames.length
                + (hasInterceptItem ? 1 : 0) + (modelType.equals(LinearModelType.AFT) ? 1 : 0));
        }

        // Loss object function
        DataSet<OptimObjFunc> objFunc = session.getExecutionEnvironment()
            .fromElements(getObjFunction(modelType, params));

        if (params.contains(LinearTrainParams.OPTIM_METHOD)) {
            OptimMethod method = OptimMethod.valueOf(params.get(LinearTrainParams.OPTIM_METHOD).toUpperCase());
            return OptimizerFactory.create(objFunc, trainData, coefficientDim, params, method).optimize();
        } else if (params.get(HasL1.L_1) > 0) {
            return new Owlqn(objFunc, trainData, coefficientDim, params).optimize();
        } else {
            return new Lbfgs(objFunc, trainData, coefficientDim, params).optimize();
        }
    }

    /**
     * Get obj function.
     *
     * @param modelType model type.
     * @param params    parameters for train.
     * @return
     */
    public static OptimObjFunc getObjFunction(LinearModelType modelType, Params params) {
        OptimObjFunc objFunc;
        // For different model type, we must set corresponding loss object function.
        switch (modelType) {
            case LinearReg:
                objFunc = new UnaryLossObjFunc(new SquareLossFunc(), params);
                break;
            case SVR:
                double svrTau = params.get(LinearSvrTrainParams.TAU);
                objFunc = new UnaryLossObjFunc(new SvrLossFunc(svrTau), params);
                break;
            case LR:
                objFunc = new UnaryLossObjFunc(new LogLossFunc(), params);
                break;
            case SVM:
                objFunc = new UnaryLossObjFunc(new SmoothHingeLossFunc(), params);
                break;
            case Perceptron:
                objFunc = new UnaryLossObjFunc(new PerceptronLossFunc(), params);
                break;
            case AFT:
                objFunc = new AftRegObjFunc(params);
                break;
            default:
                throw new RuntimeException("Not implemented yet!");
        }
        return objFunc;
    }

    /**
     * Get label info: including label values and label type.
     *
     * @param in        input train data in BatchOperator format.
     * @param params    train parameters.
     * @param isRegProc is regression process or not.
     * @return label info.
     */
    private Tuple2<DataSet<Object>, TypeInformation> getLabelInfo(BatchOperator in,
                                                                  Params params,
                                                                  boolean isRegProc) {
        String labelName = params.get(LinearTrainParams.LABEL_COL);
        // Prepare label values
        DataSet<Object> labelValues;
        TypeInformation<?> labelType = null;
        if (isRegProc) {
            labelType = Types.DOUBLE;
            labelValues = MLEnvironmentFactory.get(in.getMLEnvironmentId())
                .getExecutionEnvironment().fromElements(new Object());
        } else {
            labelType = in.getColTypes()[TableUtil.findColIndex(in.getColNames(), labelName)];
            labelValues = in.select(new String[] {labelName}).distinct().getDataSet().map(
                new MapFunction<Row, Object>() {
                    @Override
                    public Object map(Row row) {
                        return row.getField(0);
                    }
                });
        }
        return Tuple2.of(labelValues, labelType);
    }

    /**
     * Transform train data to Tuple3 format.
     *
     * @param in          train data in row format.
     * @param params      train parameters.
     * @param labelValues label values.
     * @param isRegProc   is regression process or not.
     * @return Tuple3 format train data <weight, label, vector></>.
     */
    private DataSet<Tuple3<Double, Double, Vector>> transform(BatchOperator in,
                                                              Params params,
                                                              DataSet<Object> labelValues,
                                                              boolean isRegProc) {
        String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
        String labelName = params.get(LinearTrainParams.LABEL_COL);
        String weightColName = params.get(LinearTrainParams.WEIGHT_COL);
        String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
        TableSchema dataSchema = in.getSchema();
        if (null == featureColNames && null == vectorColName) {
            featureColNames = TableUtil.getNumericCols(dataSchema, new String[] {labelName});
            params.set(LinearTrainParams.FEATURE_COLS, featureColNames);
        }
        int[] featureIndices = null;
        int labelIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), labelName);
        if (featureColNames != null) {
            featureIndices = new int[featureColNames.length];
            for (int i = 0; i < featureColNames.length; ++i) {
                int idx = TableUtil.findColIndex(in.getColNames(), featureColNames[i]);
                featureIndices[i] = idx;
                TypeInformation type = in.getSchema().getFieldTypes()[idx];

                Preconditions.checkState(TableUtil.isNumber(type),
                    "linear algorithm only support numerical data type. type is : " + type);
            }
        }
        int weightIdx = weightColName != null ? TableUtil.findColIndex(in.getColNames(), weightColName) : -1;
        int vecIdx = vectorColName != null ? TableUtil.findColIndex(in.getColNames(), vectorColName) : -1;

        return in.getDataSet().map(new Transform(isRegProc, weightIdx, vecIdx, featureIndices, labelIdx))
            .withBroadcastSet(labelValues, LABEL_VALUES);
    }

    /**
     * Get feature types.
     *
     * @param in              train data.
     * @param featureColNames feature column names.
     * @return feature types.
     */
    private String[] getFeatureTypes(BatchOperator in, String[] featureColNames) {
        if (featureColNames != null) {
            String[] featureColTypes = new String[featureColNames.length];
            for (int i = 0; i < featureColNames.length; ++i) {
                int idx = TableUtil.findColIndex(in.getColNames(), featureColNames[i]);
                TypeInformation type = in.getSchema().getFieldTypes()[idx];
                if (type.equals(Types.DOUBLE)) {
                    featureColTypes[i] = "double";
                } else if (type.equals(Types.FLOAT)) {
                    featureColTypes[i] = "float";
                } else if (type.equals(Types.LONG)) {
                    featureColTypes[i] = "long";
                } else if (type.equals(Types.INT)) {
                    featureColTypes[i] = "int";
                } else if (type.equals(Types.SHORT)) {
                    featureColTypes[i] = "short";
                } else if (type.equals(Types.BOOLEAN)) {
                    featureColTypes[i] = "bool";
                } else {
                    throw new RuntimeException(
                        "linear algorithm only support numerical data type. type is : " + type);
                }
            }
            return featureColTypes;
        }
        return null;
    }

    /**
     * Do standardization and interception to train data.
     *
     * @param initData initial data.
     * @param params   train parameters.
     * @param meanVar  mean and variance of train data.
     * @return train data after standardization.
     */
    private DataSet<Tuple3<Double, Double, Vector>> preProcess(
        DataSet<Tuple3<Double, Double, Vector>> initData,
        Params params,
        DataSet<DenseVector[]> meanVar) {
        // Get parameters.
        final boolean hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
        final boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);

        return initData.map(
            new RichMapFunction<Tuple3<Double, Double, Vector>, Tuple3<Double, Double, Vector>>() {
                private DenseVector[] meanVar;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.meanVar = (DenseVector[])getRuntimeContext()
                        .getBroadcastVariable(MEAN_VAR).get(0);
                    modifyMeanVar(standardization, meanVar);
                }

                @Override
                public Tuple3<Double, Double, Vector> map(Tuple3<Double, Double, Vector> value)
                    throws Exception {

                    Vector aVector = value.f2;
                    if (aVector instanceof DenseVector) {
                        DenseVector bVector;
                        if (standardization) {
                            if (hasInterceptItem) {
                                bVector = new DenseVector(aVector.size() + 1);
                                bVector.set(0, 1.0);
                                for (int i = 0; i < aVector.size(); ++i) {
                                    bVector.set(i + 1, (aVector.get(i) - meanVar[0].get(i)) / meanVar[1].get(i));
                                }
                            } else {
                                bVector = (DenseVector)aVector;
                                for (int i = 0; i < aVector.size(); ++i) {
                                    bVector.set(i, aVector.get(i) / meanVar[1].get(i));
                                }
                            }
                        } else {
                            if (hasInterceptItem) {
                                bVector = new DenseVector(aVector.size() + 1);
                                bVector.set(0, 1.0);
                                for (int i = 0; i < aVector.size(); ++i) {
                                    bVector.set(i + 1, aVector.get(i));
                                }
                            } else {
                                bVector = (DenseVector)aVector;
                            }
                        }
                        return Tuple3.of(value.f0, value.f1, bVector);

                    } else {
                        SparseVector bVector = (SparseVector)aVector;

                        if (standardization) {
                            if (hasInterceptItem) {

                                int[] indices = bVector.getIndices();
                                double[] vals = bVector.getValues();
                                for (int i = 0; i < indices.length; ++i) {
                                    vals[i] = (vals[i] - meanVar[0].get(indices[i])) / meanVar[1].get(
                                        indices[i]);
                                }
                                bVector = bVector.prefix(1.0);
                            } else {
                                int[] indices = bVector.getIndices();
                                double[] vals = bVector.getValues();
                                for (int i = 0; i < indices.length; ++i) {
                                    vals[i] = vals[i] / meanVar[1].get(indices[i]);
                                }
                            }
                        } else {
                            if (hasInterceptItem) {
                                bVector = bVector.prefix(1.0);
                            }
                        }
                        return Tuple3.of(value.f0, value.f1, bVector);
                    }
                }
            }).withBroadcastSet(meanVar, MEAN_VAR);
    }

    /**
     * In this function, we do some parameters transformation, just like lambda, tau,
     * and return the type of training: regression or classification.
     *
     * @param params          parameters for linear train.
     * @param linearModelType linear model type.
     * @param modelName       model name.
     * @return training is regression or not.
     */
    private static boolean getIsRegProc(Params params, LinearModelType linearModelType, String modelName) {
        if (linearModelType.equals(LinearModelType.LinearReg)) {
            if ("Ridge Regression".equals(modelName)) {
                double lambda = params.get(RidgeRegTrainParams.LAMBDA);
                Preconditions.checkState((lambda > 0), "lambda must be positive number or zero! lambda is : " + lambda);
                params.set(ModelParamName.L2, lambda);
                params.remove(RidgeRegTrainParams.LAMBDA);
            } else if ("LASSO".equals(modelName)) {
                double lambda = params.get(LassoRegTrainParams.LAMBDA);
                if (lambda < 0) {
                    throw new RuntimeException("lambda must be positive number or zero!");
                }
                params.set(ModelParamName.L1, lambda);
                params.remove(RidgeRegTrainParams.LAMBDA);
            }
            return true;
        } else if (linearModelType.equals(LinearModelType.SVR)) {
            Double tau = params.get(LinearSvrTrainParams.TAU);
            double cParam = params.get(LinearSvrTrainParams.C);
            if (tau < 0) {
                throw new RuntimeException("Parameter tau must be positive number or zero!");
            }
            if (cParam <= 0) {
                throw new RuntimeException("Parameter C must be positive number!");
            }
            params.remove(ModelParamName.L1);
            params.remove(ModelParamName.L2);
            params.set(ModelParamName.L2, 1.0 / cParam);
            params.remove(LinearSvrTrainParams.C);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Build model data.
     *
     * @param meta            meta info.
     * @param featureNames    feature column names.
     * @param labelType       label type.
     * @param meanVar         mean and variance of vector.
     * @param hasIntercept    has interception or not.
     * @param standardization do standardization or not.
     * @param coefVector      coefficient vector.
     * @return linear mode data.
     */
    public static LinearModelData buildLinearModelData(Params meta,
                                                       String[] featureNames,
                                                       TypeInformation labelType,
                                                       DenseVector[] meanVar,
                                                       boolean hasIntercept,
                                                       boolean standardization,
                                                       Tuple2<DenseVector, double[]> coefVector) {
        if (!(LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)))) {
            modifyMeanVar(standardization, meanVar);
        }

        meta.set(ModelParamName.VECTOR_SIZE, coefVector.f0.size()
            - (meta.get(ModelParamName.HAS_INTERCEPT_ITEM) ? 1 : 0)
            - (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE).toString()) ? 1 : 0));
        if (!(LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)))) {
            if (standardization) {
                int n = meanVar[0].size();
                if (hasIntercept) {
                    double sum = 0.0;
                    for (int i = 0; i < n; ++i) {
                        sum += coefVector.f0.get(i + 1) * meanVar[0].get(i) / meanVar[1].get(i);
                        coefVector.f0.set(i + 1, coefVector.f0.get(i + 1) / meanVar[1].get(i));
                    }
                    coefVector.f0.set(0, coefVector.f0.get(0) - sum);
                } else {
                    for (int i = 0; i < n; ++i) {
                        coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(i));
                    }
                }
            }
        }

        LinearModelData modelData = new LinearModelData(labelType, meta, featureNames, coefVector.f0);
        modelData.lossCurve = coefVector.f1;

        return modelData;
    }

    /**
     * Create meta info.
     */
    public static class CreateMeta implements MapPartitionFunction<Object, Params> {
        private String modelName;
        private LinearModelType modelType;
        private boolean hasInterceptItem;
        private boolean isRegProc;
        private String vectorColName;
        private String labelName;

        public CreateMeta(String modelName, LinearModelType modelType,
                          boolean isRegProc, Params params) {
            this.modelName = modelName;
            this.modelType = modelType;
            this.hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
            this.isRegProc = isRegProc;
            this.vectorColName = params.get(LinearTrainParams.VECTOR_COL);
            this.labelName = params.get(LinearTrainParams.LABEL_COL);
        }

        @Override
        public void mapPartition(Iterable<Object> rows, Collector<Params> metas) throws Exception {
            Object[] labels = null;
            if (!this.isRegProc) {
                labels = orderLabels(rows);
            }

            Params meta = new Params();
            meta.set(ModelParamName.MODEL_NAME, this.modelName);
            meta.set(ModelParamName.LINEAR_MODEL_TYPE, this.modelType);
            meta.set(ModelParamName.LABEL_VALUES, labels);
            meta.set(ModelParamName.HAS_INTERCEPT_ITEM, this.hasInterceptItem);
            meta.set(ModelParamName.VECTOR_COL_NAME, vectorColName);
            meta.set(LinearTrainParams.LABEL_COL, labelName);
            metas.collect(meta);
        }
    }

    /**
     * Transform the train data to Tuple3 format: Tuple3<weightValue, labelValue, featureVector>
     */
    private static class Transform extends RichMapFunction<Row, Tuple3<Double, Double, Vector>> {
        private String positiveLableValueString = null;

        private boolean isRegProc;
        private int weightIdx;
        private int vecIdx;
        private int labelIdx;
        private int[] featureIndices;

        public Transform(boolean isRegProc, int weightIdx, int vecIdx, int[] featureIndices, int labelIdx) {
            this.isRegProc = isRegProc;
            this.weightIdx = weightIdx;
            this.vecIdx = vecIdx;
            this.featureIndices = featureIndices;
            this.labelIdx = labelIdx;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            if (!this.isRegProc) {
                List<Object> labelRows = getRuntimeContext().getBroadcastVariable(LABEL_VALUES);
                this.positiveLableValueString = orderLabels(labelRows)[0].toString();
            }
        }

        @Override
        public Tuple3<Double, Double, Vector> map(Row row) throws Exception {
            Double weight = weightIdx != -1 ? ((Number)row.getField(weightIdx)).doubleValue() : 1.0;
            Double val = FeatureLabelUtil.getLabelValue(row, this.isRegProc,
                labelIdx, this.positiveLableValueString);
            if (featureIndices != null) {
                DenseVector vec = new DenseVector(featureIndices.length);
                for (int i = 0; i < featureIndices.length; ++i) {
                    vec.set(i, ((Number)row.getField(featureIndices[i])).doubleValue());
                }
                return Tuple3.of(weight, val, vec);
            } else {
                Vector vec = VectorUtil.getVector(row.getField(vecIdx));
                Preconditions.checkState((vec != null),
                    "vector for linear model train is null, please check your input data.");

                return Tuple3.of(weight, val, vec);
            }

        }
    }

    /**
     * The size of coefficient. Transform dimension of trainData, if has Intercept item, dimension++,
     * if modelType is Aft, then dimension++,
     * because there is another coefficient so that coefficient size is one more larger then data size.
     */
    private static class DimTrans extends AbstractRichFunction
        implements MapFunction<Integer, Integer> {
        private boolean hasInterceptItem;
        private LinearModelType linearModelType;
        private Integer featureDim = null;

        public DimTrans(boolean hasInterceptItem, LinearModelType linearModelType) {
            this.hasInterceptItem = hasInterceptItem;
            this.linearModelType = linearModelType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.featureDim = (Integer)getRuntimeContext()
                .getBroadcastVariable(VECTOR_SIZE).get(0);
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            return this.featureDim + (this.hasInterceptItem ? 1 : 0)
                + (this.linearModelType.equals(LinearModelType.AFT) ? 1 : 0);
        }
    }

    /**
     * build the linear model rows, the format to be output.
     */
    public static class BuildModelFromCoefs extends AbstractRichFunction implements
        MapPartitionFunction<Tuple2<DenseVector, double[]>, Row> {
        private Params meta;
        private String[] featureNames;
        private String[] featureColTypes;
        private TypeInformation labelType;
        private DenseVector[] meanVar;
        private boolean hasIntercept;
        private boolean standardization;

        public BuildModelFromCoefs(TypeInformation labelType, String[] featureNames,
                                   boolean standardization,
                                   boolean hasIntercept,
                                   String[] featureColTypes) {
            this.labelType = labelType;
            this.featureNames = featureNames;
            this.standardization = standardization;
            this.hasIntercept = hasIntercept;
            this.featureColTypes = featureColTypes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.meta = (Params)getRuntimeContext().getBroadcastVariable(META).get(0);
            this.meta.set(ModelParamName.FEATURE_TYPES, featureColTypes);
            if (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE))) {
                this.meanVar = null;
            } else {
                this.meanVar = (DenseVector[])getRuntimeContext().getBroadcastVariable(MEAN_VAR).get(0);
            }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<DenseVector, double[]>> iterable,
                                 Collector<Row> collector) throws Exception {
            for (Tuple2<DenseVector, double[]> coefVector : iterable) {
                LinearModelData modelData = buildLinearModelData(meta,
                    featureNames,
                    labelType,
                    meanVar,
                    hasIntercept,
                    standardization,
                    coefVector);

                new LinearModelDataConverter(this.labelType).save(modelData, collector);
            }
        }
    }

    /**
     * modify mean and variance, if variance equals zero, then modify them.
     *
     * @param standardization do standardization or not.
     * @param meanVar         mean and variance.
     */
    private static void modifyMeanVar(boolean standardization, DenseVector[] meanVar) {
        if (standardization) {
            for (int i = 0; i < meanVar[1].size(); ++i) {
                if (meanVar[1].get(i) == 0) {
                    meanVar[1].set(i, 1.0);
                    meanVar[0].set(i, 0.0);
                }
            }
        }
    }
}



