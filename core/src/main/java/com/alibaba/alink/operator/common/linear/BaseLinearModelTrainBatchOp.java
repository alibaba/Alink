package com.alibaba.alink.operator.common.linear;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.PerceptronLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SmoothHingeLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SquareLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SvrLossFunc;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.OptimizerFactory;
import com.alibaba.alink.operator.common.optim.Owlqn;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class of linear model training. Linear binary classification and linear regression algorithms should inherit
 * this class. Then it only need to write the code of loss function and regular item.
 *
 * @param <T> parameter of this class. Maybe the Svm, linearRegression or Lr parameter.
 */

@Internal
public abstract class BaseLinearModelTrainBatchOp<T extends BaseLinearModelTrainBatchOp<T>> extends BatchOperator<T>
        implements WithTrainInfo<LinearModelTrainInfo, T> {
    private static final long serialVersionUID = 6162495789625212086L;
    private String modelName;
    private LinearModelType linearModelType;
    private static final String META = "meta";
    private static final String MEAN_VAR = "meanVar";
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
        final boolean isRegProc = getIsRegProc(params, linearModelType, modelName);
        final boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);
        TypeInformation labelType = isRegProc ? Types.DOUBLE : in.getColTypes()[TableUtil
                .findColIndexWithAssertAndHint(in.getColNames(), params.get(LinearTrainParams.LABEL_COL))];

        // Transform data to Tuple3 format <weight, label, feature vector>.
        DataSet<Tuple3<Double, Object, Vector>> initData = transform(in, params, isRegProc, standardization);

        DataSet<Tuple3<DenseVector[], Object[], Integer>> utilInfo = getUtilInfo(initData, standardization,
                isRegProc);

        DataSet<DenseVector[]> meanVar = utilInfo.map(
                new MapFunction<Tuple3<DenseVector[], Object[], Integer>, DenseVector[]>() {
                    private static final long serialVersionUID = 7127767376687624403L;

                    @Override
                    public DenseVector[] map(Tuple3<DenseVector[], Object[], Integer> value)
                            throws Exception {
                        return value.f0;
                    }
                });

        DataSet<Integer> featSize = utilInfo.map(
                new MapFunction<Tuple3<DenseVector[], Object[], Integer>, Integer>() {
                    private static final long serialVersionUID = 2773811388068064638L;

                    @Override
                    public Integer map(Tuple3<DenseVector[], Object[], Integer> value)
                            throws Exception {
                        return value.f2;
                    }
                });

        DataSet<Object[]> labelValues = utilInfo.flatMap(
                new FlatMapFunction<Tuple3<DenseVector[], Object[], Integer>, Object[]>() {

                    private static final long serialVersionUID = 5375954526931728363L;

                    @Override
                    public void flatMap(Tuple3<DenseVector[], Object[], Integer> value,
                                        Collector<Object[]> out)
                            throws Exception {
                        if (!isRegProc) {
                            Preconditions.checkState((value.f1.length == 2),
                                    "labels count should be 2 in in classification algo.");
                        }
                        out.collect(value.f1);
                    }
                });

        DataSet<Tuple3<Double, Double, Vector>>
                trainData = preProcess(initData, params, isRegProc, meanVar, labelValues, featSize);
        // Solve the optimization problem.
        DataSet<Tuple2<DenseVector, double[]>> coefVectorSet = optimize(params, featSize,
                trainData, linearModelType, MLEnvironmentFactory.get(getMLEnvironmentId()));
        //// Prepare the meta info of linear model.
        DataSet<Params> meta = labelValues
                .mapPartition(new CreateMeta(modelName, linearModelType, params))
                .setParallelism(1);
        // Build linear model rows, the format to be output.
        DataSet<Row> modelRows;
        String[] featureColTypes = getFeatureTypes(in, params.get(LinearTrainParams.FEATURE_COLS));
        modelRows = coefVectorSet
                .mapPartition(new BuildModelFromCoefs(labelType,
                        params.get(LinearTrainParams.FEATURE_COLS),
                        params.get(LinearTrainParams.STANDARDIZATION),
                        params.get(LinearTrainParams.WITH_INTERCEPT), featureColTypes))
                .withBroadcastSet(meta, META)
                .withBroadcastSet(meanVar, MEAN_VAR)
                .setParallelism(1);
        // Convert the model rows to table.
        this.setOutput(modelRows, new LinearModelDataConverter(labelType).getModelSchema());

        this.setSideOutputTables(getSideTablesOfCoefficient(modelRows, initData, featSize,
                params.get(LinearTrainParams.FEATURE_COLS),
                params.get(LinearTrainParams.WITH_INTERCEPT),
                getMLEnvironmentId()));
        return (T) this;
    }

    public static DataSet<Tuple3<DenseVector[], Object[], Integer>> getUtilInfo(
            DataSet<Tuple3<Double, Object, Vector>> initData,
            boolean standardization,
            boolean isRegProc) {
        return initData.filter(
                new FilterFunction<Tuple3<Double, Object, Vector>>() {
                    private static final long serialVersionUID = 4129133776653527498L;

                    @Override
                    public boolean filter(Tuple3<Double, Object, Vector> value) throws Exception {
                        if (value.f0 < 0.0) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).reduceGroup(
                new GroupReduceFunction<Tuple3<Double, Object, Vector>, Tuple3<DenseVector[], Object[],
                        Integer>>() {
                    private static final long serialVersionUID = -4819473589070441623L;

                    @Override
                    public void reduce(Iterable<Tuple3<Double, Object, Vector>> values,
                                       Collector<Tuple3<DenseVector[], Object[], Integer>> out) {
                        int sparseSize = -1;
                        int denseSize = -1;
                        Set<Object> labelValues = new HashSet<>();
                        DenseVector sparseMeanVar = null;
                        DenseVector denseMeanVar = null;
                        boolean hasSparseVector = false;
                        boolean hasDenseVector = false;
                        boolean calcSparseMeanVar = false;
                        boolean calcDenseMeanVar = false;

                        List<Tuple3<Double, Object, Vector>> denseList = new ArrayList<>();
                        List<Tuple3<Double, Object, Vector>> sparseList = new ArrayList<>();
                        for (Tuple3<Double, Object, Vector> value : values) {
                            if (value.f0 == -1) {
                                sparseList.add(value);
                                hasSparseVector = true;
                            } else if (value.f0 == -2) {
                                denseList.add(value);
                                hasDenseVector = true;
                            }
                        }
                        if (hasSparseVector) {
                            for (Tuple3<Double, Object, Vector> value : sparseList) {
                                Tuple2<Integer, Object[]>
                                        labelVals = (Tuple2<Integer, Object[]>) value.f1;
                                for (int i = 0; i < labelVals.f1.length; ++i) {
                                    labelValues.add(labelVals.f1[i]);
                                }
                                if (sparseMeanVar == null) {
                                    sparseMeanVar = (DenseVector) value.f2;
                                    calcSparseMeanVar = (sparseMeanVar != null && sparseMeanVar.size() != 0);
                                    sparseSize = labelVals.f0;
                                } else if (labelVals.f0 == sparseSize) {
                                    if (calcSparseMeanVar) {
                                        for (int i = 0; i < sparseMeanVar.size(); ++i) {
                                            sparseMeanVar.set(i, Math.max(sparseMeanVar.get(i),
                                                    Math.abs(value.f2.get(i))));
                                        }
                                    }
                                } else {
                                    if (calcSparseMeanVar) {
                                        if (labelVals.f0 < sparseSize) {
                                            for (int i = 0; i < value.f0; ++i) {
                                                sparseMeanVar.set(i, Math.max(sparseMeanVar.get(i),
                                                        Math.abs(value.f2.get(i))));
                                            }
                                        } else {
                                            for (int i = 0; i < sparseSize; ++i) {
                                                value.f2.set(i, Math.max(Math.abs(value.f2.get(i)),
                                                        sparseMeanVar.get(i)));
                                            }
                                            sparseMeanVar = (DenseVector) value.f2;
                                            sparseSize = labelVals.f0.intValue();
                                        }
                                    }
                                }
                            }
                        }

                        if (hasDenseVector) {
                            for (Tuple3<Double, Object, Vector> value : denseList) {
                                Tuple2<Integer, Object[]>
                                        labelVals = (Tuple2<Integer, Object[]>) value.f1;
                                for (int i = 0; i < labelVals.f1.length; ++i) {
                                    labelValues.add(labelVals.f1[i]);
                                }
                                if (denseMeanVar == null) {
                                    denseMeanVar = (DenseVector) value.f2;
                                    calcDenseMeanVar = (denseMeanVar != null && denseMeanVar.size() != 0);
                                    denseSize = labelVals.f0;
                                } else if (labelVals.f0 == denseSize) {
                                    if (calcDenseMeanVar) {
                                        for (int i = 0; i < denseSize; ++i) {
                                            denseMeanVar.set(i, denseMeanVar.get(i) + value.f2.get(i));
                                            denseMeanVar.set(denseSize + i, denseMeanVar.get(denseSize + i)
                                                    + value.f2.get(denseSize + i));
                                            denseMeanVar.set(2 * denseSize + i, Math.max(denseMeanVar.get(2 *
                                                    denseSize + i), Math.abs(value.f2.get(2 * denseSize + i))));
                                        }
                                        denseMeanVar.set(3 * denseSize, denseMeanVar.get(3 * denseSize)
                                                + value.f2.get(3 * denseSize));
                                    }
                                } else if (labelVals.f0 < denseSize) {
                                    if (calcDenseMeanVar) {
                                        for (int i = 0; i < labelVals.f0; ++i) {
                                            denseMeanVar.set(i, denseMeanVar.get(i) + value.f2.get(i));
                                            denseMeanVar.set(denseSize + i, denseMeanVar.get(denseSize + i)
                                                    + value.f2.get(denseSize + i));
                                            denseMeanVar.set(2 * denseSize + i, Math.max(denseMeanVar.get(2 *
                                                    denseSize + i), Math.abs(value.f2.get(2 * denseSize + i))));
                                        }
                                        denseMeanVar.set(3 * denseSize, denseMeanVar.get(3 * denseSize)
                                                + value.f2.get(3 * denseSize));
                                    }
                                } else {
                                    if (calcDenseMeanVar) {
                                        for (int i = 0; i < denseSize; ++i) {
                                            value.f2.set(i, denseMeanVar.get(i) + value.f2.get(i));
                                            value.f2.set(denseSize + i, denseMeanVar.get(denseSize + i)
                                                    + value.f2.get(denseSize + i));
                                            value.f2.set(2 * denseSize + i, Math.max(denseMeanVar.get(2 *
                                                    denseSize + i), Math.abs(value.f2.get(2 * denseSize + i))));
                                        }
                                        value.f2.set(3 * denseSize, denseMeanVar.get(3 * denseSize)
                                                + value.f2.get(3 * denseSize));
                                        denseMeanVar = (DenseVector) value.f2;
                                    }
                                    denseSize = labelVals.f0;
                                }

                            }
                        }

                        boolean calMeanVar = calcDenseMeanVar || calcSparseMeanVar;

                        if (hasSparseVector && hasDenseVector) {
                            if (calMeanVar) {
                                if (sparseMeanVar.size() >= denseMeanVar.size() / 3) {
                                    for (int i = 0; i < sparseSize; ++i) {
                                        sparseMeanVar.set(i, Math.max(sparseMeanVar.get(i),
                                                Math.abs(denseMeanVar.get(2 * sparseSize + i))));
                                    }
                                } else {
                                    DenseVector newMeanVar = new DenseVector(denseSize);
                                    for (int i = 0; i < sparseMeanVar.size(); ++i) {
                                        newMeanVar.set(i, sparseMeanVar.get(i));
                                    }
                                    for (int i = 0; i < denseSize; ++i) {
                                        newMeanVar.set(i, Math.max(newMeanVar.get(i),
                                                Math.abs(denseMeanVar.get(2 * denseSize + i))));
                                    }
                                    sparseMeanVar = newMeanVar;
                                }
                            }
                        } else if (hasDenseVector && (!hasSparseVector)) {
                            sparseMeanVar = denseMeanVar;
                        }


                        int size = Math.max(sparseSize, denseSize);
                        DenseVector[] meanAndVar = new DenseVector[2];
                        Object[] labelssort = isRegProc ? labelValues.toArray() : orderLabels(labelValues);
                        meanAndVar[0] = calMeanVar ? new DenseVector(size) : new DenseVector(0);
                        meanAndVar[1] = calMeanVar ? new DenseVector(size) : new DenseVector(0);

                        if (calMeanVar) {
                            if (hasSparseVector) {
                                meanAndVar[1] = sparseMeanVar;
                                modifyMeanVar(standardization, meanAndVar);
                            } else if (hasDenseVector) {
                                for (int i = 0; i < size; ++i) {
                                    meanAndVar[0].set(i, sparseMeanVar.get(i) / sparseMeanVar.get(3 * size));
                                    meanAndVar[1].set(i, sparseMeanVar.get(size + i) - sparseMeanVar.get(3
                                            * size) * meanAndVar[0].get(i) * meanAndVar[0].get(i));
                                }
                                for (int i = 0; i < size; ++i) {
                                    meanAndVar[1].set(i, Math.max(0.0, meanAndVar[1].get(i)));
                                    meanAndVar[1].set(i, Math.sqrt(meanAndVar[1].get(i) / (sparseMeanVar.get(3
                                            * size) - 1)));
                                }
                                modifyMeanVar(standardization, meanAndVar);
                            }
                        }

                        out.collect(Tuple3.of(meanAndVar, labelssort, size));
                    }
                });
    }

    public static Table[] getSideTablesOfCoefficient(DataSet<Row> modelRow,
                                                     DataSet<Tuple3<Double, Object, Vector>> inputData,
                                                     DataSet<Integer> vecSize,
                                                     final String[] featureNames,
                                                     final boolean hasInterception,
                                                     long environmentId) {
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

        DataSet<Tuple5<String, String[], double[], double[], double[]>> allInfo = inputData
                .mapPartition(
                        new RichMapPartitionFunction<Tuple3<Double, Object, Vector>, Tuple3<Integer, double[], double[]>>
                                () {
                            private static final long serialVersionUID = 8785824618242390100L;
                            private int vectorSize;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                this.vectorSize = (int) getRuntimeContext().getBroadcastVariable("vectorSize").get(0);
                                if (hasInterception) {
                                    vectorSize--;
                                }
                            }

                            @Override
                            public void mapPartition(Iterable<Tuple3<Double, Object, Vector>> values,
                                                     Collector<Tuple3<Integer, double[], double[]>> out) throws Exception {
                                int iter = 0;
                                double[] mu = new double[vectorSize];
                                double[] mu2 = new double[vectorSize];
                                if (featureNames == null) {
                                    for (Tuple3<Double, Object, Vector> t3 : values) {
                                        if (t3.f0 < 0.0) {
                                            continue;
                                        }
                                        if (t3.f2 instanceof SparseVector) {
                                            SparseVector tmp = (SparseVector) t3.f2;
                                            tmp.setSize(vectorSize);

                                            double[] vals = tmp.getValues();
                                            int[] idx = tmp.getIndices();
                                            for (int i = 0; i < vals.length; ++i) {
                                                if (hasInterception) {
                                                    if (idx[i] > 0) {
                                                        mu[idx[i] - 1] += vals[i];
                                                        mu2[idx[i] - 1] += vals[i] * vals[i];
                                                    }
                                                } else {
                                                    mu[idx[i]] += vals[i];
                                                    mu2[idx[i]] += vals[i] * vals[i];
                                                }
                                            }
                                            iter++;
                                        } else {
                                            for (int i = 0; i < vectorSize; ++i) {
                                                double val = t3.f2.get(i + (hasInterception ? 1 : 0));
                                                mu[i] += val;
                                                mu2[i] += val * val;
                                            }
                                            iter++;
                                        }
                                    }
                                } else {
                                    for (Tuple3<Double, Object, Vector> t3 : values) {
                                        if (t3.f0 < 0.0) {
                                            continue;
                                        }
                                        for (int i = 0; i < vectorSize; ++i) {
                                            double val = t3.f2.get(i + (hasInterception ? 1 : 0));
                                            mu[i] += val;
                                            mu2[i] += val * val;
                                        }
                                        iter++;
                                    }
                                }
                                out.collect(Tuple3.of(iter, mu, mu2));
                            }
                        }).withBroadcastSet(vecSize, "vectorSize")
                .reduce(new ReduceFunction<Tuple3<Integer, double[], double[]>>() {
                    private static final long serialVersionUID = 7062783877162095989L;

                    @Override
                    public Tuple3<Integer, double[], double[]> reduce(Tuple3<Integer, double[], double[]> t1,
                                                                      Tuple3<Integer, double[], double[]> t2)
                            throws Exception {
                        t2.f0 = t1.f0 + t2.f0;
                        for (int i = 0; i < t1.f1.length; ++i) {
                            t2.f1[i] = t1.f1[i] + t2.f1[i];
                            t2.f2[i] = t1.f2[i] + t2.f2[i];
                        }

                        return t2;
                    }
                })
                .flatMap(new RichFlatMapFunction<Tuple3<Integer, double[], double[]>,
                        Tuple5<String, String[], double[], double[], double[]>>() {
                    private static final long serialVersionUID = 7815111101106759520L;
                    private DenseVector coefVec;
                    private LinearModelData model;
                    private double[] cinfo;
                    private String[] colNames;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        model = ((LinearModelData) getRuntimeContext().getBroadcastVariable("model").get(0));
                        coefVec = model.coefVector;
                        cinfo = model.convergenceInfo;
                    }

                    @Override
                    public void flatMap(Tuple3<Integer, double[], double[]> value,
                                        Collector<Tuple5<String, String[], double[], double[], double[]>> out)
                            throws Exception {
                        double[] importance;
                        if (featureNames == null) {
                            colNames = new String[coefVec.size() - (hasInterception ? 1 : 0)];
                            for (int i = 0; i < colNames.length; ++i) {
                                colNames[i] = String.valueOf(i);
                            }
                        } else {
                            colNames = featureNames;
                        }

                        if (hasInterception) {
                            importance = new double[coefVec.size() - 1];
                        } else {
                            importance = new double[coefVec.size()];
                        }
                        for (int i = 0; i < value.f1.length; ++i) {

                            double nu = value.f1[i] / value.f0;
                            double sigma = value.f2[i] - value.f0 * nu * nu;
                            if (value.f0 == 1) {
                                sigma = 0.0;
                            } else {
                                sigma = Math.sqrt(Math.max(0.0, sigma) / (value.f0 - 1));
                            }
                            importance[i] = Math.abs(coefVec.get(i + (hasInterception ? 1 : 0)) * sigma);
                        }

                        out.collect(
                                Tuple5.of(JsonConverter.toJson(model.getMetaInfo()), colNames, coefVec.getData(),
                                        importance, cinfo));

                    }
                }).setParallelism(1).withBroadcastSet(model, "model");

        DataSet<Row> importance = allInfo.mapPartition(
                new MapPartitionFunction<Tuple5<String, String[], double[], double[], double[]>, Row>() {
                    private static final long serialVersionUID = -3263497114974298286L;

                    @Override
                    public void mapPartition(Iterable<Tuple5<String, String[], double[], double[], double[]>> tuple5s,
                                             Collector<Row> out) throws Exception {

                        String[] colNames = null;
                        double[] importanceVals = null;
                        for (Tuple5<String, String[], double[], double[], double[]> r : tuple5s) {
                            colNames = r.f1;
                            importanceVals = r.f3;
                        }

                        for (int i = 0; i < colNames.length; ++i) {
                            out.collect(Row.of(colNames[i], importanceVals[i]));
                        }
                    }
                });
        DataSet<Row> weights = allInfo.mapPartition(
                new MapPartitionFunction<Tuple5<String, String[], double[], double[], double[]>, Row>() {
                    private static final long serialVersionUID = -6164289179429722407L;

                    @Override
                    public void mapPartition(Iterable<Tuple5<String, String[], double[], double[], double[]>> tuple5s,
                                             Collector<Row> out) throws Exception {
                        String[] colNames = null;
                        double[] weights = null;
                        for (Tuple5<String, String[], double[], double[], double[]> r : tuple5s) {
                            colNames = r.f1;
                            weights = r.f2;
                        }
                        if (weights.length == colNames.length) {
                            for (int i = 0; i < colNames.length; ++i) {
                                out.collect(Row.of(colNames[i], weights[i]));
                            }
                        } else {
                            out.collect(Row.of("_intercept_", weights[0]));
                            for (int i = 0; i < colNames.length; ++i) {
                                out.collect(Row.of(colNames[i], weights[i + 1]));
                            }
                        }
                    }
                });

        DataSet<Row> summary = allInfo.mapPartition(
                new MapPartitionFunction<Tuple5<String, String[], double[], double[], double[]>, Row>() {
                    private static final long serialVersionUID = -6164289179429722407L;
                    private final static int NUM_COLLECT_THRESHOLD = 10000;

                    @Override
                    public void mapPartition(Iterable<Tuple5<String, String[], double[], double[], double[]>> tuple5s,
                                             Collector<Row> out) throws Exception {

                        for (Tuple5<String, String[], double[], double[], double[]> r : tuple5s) {
                            if (r.f1.length < NUM_COLLECT_THRESHOLD) {
                                out.collect(Row.of(0, r.f0));
                                out.collect(Row.of(1, JsonConverter.toJson(r.f1)));
                                out.collect(Row.of(2, JsonConverter.toJson(r.f2)));
                                out.collect(Row.of(3, JsonConverter.toJson(r.f3)));
                                out.collect(Row.of(4, JsonConverter.toJson(r.f4)));
                            } else {
                                List<Tuple3<String, Double, Double>> array = new ArrayList<>(r.f1.length);
                                int startIdx = hasInterception ? 1 : 0;
                                for (int i = 0; i < r.f1.length; ++i) {
                                    array.add(Tuple3.of(r.f1[i], r.f2[i + startIdx], r.f3[i]));
                                }
                                array.sort(compare);
                                String[] colName = new String[NUM_COLLECT_THRESHOLD];
                                double[] weight = new double[NUM_COLLECT_THRESHOLD];
                                double[] importance = new double[NUM_COLLECT_THRESHOLD];
                                for (int i = 0; i < NUM_COLLECT_THRESHOLD / 2; ++i) {
                                    colName[i] = array.get(i).f0;
                                    weight[i] = array.get(i).f1;
                                    importance[i] = array.get(i).f2;
                                    int srcIdx = r.f1.length - i - 1;
                                    int destIdx = NUM_COLLECT_THRESHOLD - i - 1;
                                    colName[destIdx] = array.get(srcIdx).f0;
                                    weight[destIdx] = array.get(srcIdx).f1;
                                    importance[destIdx] = array.get(srcIdx).f2;
                                }

                                out.collect(Row.of(0, r.f0));
                                out.collect(Row.of(1, JsonConverter.toJson(colName)));
                                out.collect(Row.of(2, JsonConverter.toJson(weight)));
                                out.collect(Row.of(3, JsonConverter.toJson(importance)));
                                out.collect(Row.of(4, JsonConverter.toJson(r.f4)));
                            }
                        }
                    }
                });

        Table summaryTable = DataSetConversionUtil.toTable(environmentId, summary, new TableSchema(
                new String[]{"id", "info"}, new TypeInformation[]{Types.INT, Types.STRING}));
        Table importanceTable = DataSetConversionUtil.toTable(environmentId, importance, new TableSchema(
                new String[]{"col_name", "importance"}, new TypeInformation[]{Types.STRING, Types.DOUBLE}));
        Table weightTable = DataSetConversionUtil.toTable(environmentId, weights, new TableSchema(
                new String[]{"col_name", "weight"}, new TypeInformation[]{Types.STRING, Types.DOUBLE}));
        return new Table[]{summaryTable, importanceTable, weightTable};
    }

    public static Comparator compare = new Comparator<Tuple3<String, Double, Double>>() {
        @Override
        public int compare(Tuple3<String, Double, Double> o1, Tuple3<String, Double, Double> o2) {
            if (o1.f2 < o2.f2) {
                return 1;
            } else if (o1.f2 > o2.f2) {
                return -1;
            } else {
                return 0;
            }
        }
    };

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

        Preconditions.checkState((labels.length >= 2), "labels count should be more than 2 in classification algo.");
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
                                                                  DataSet<Tuple3<Double, Double, Vector>>
                                                                          trainData,
                                                                  final LinearModelType modelType,
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
            coefficientDim = vectorSize
                    .map(new MapFunction<Integer, Integer>() {
                        private static final long serialVersionUID = 5249103591725412746L;

                        @Override
                        public Integer map(Integer value) throws Exception {
                            return value + (modelType.equals(LinearModelType.AFT) ? 1 : 0);
                        }
                    });
        } else {
            coefficientDim = session.getExecutionEnvironment().fromElements(featureColNames.length
                    + (hasInterceptItem ? 1 : 0) + (modelType.equals(LinearModelType.AFT) ? 1 : 0));
        }
        // Loss object function
        DataSet<OptimObjFunc> objFunc = session.getExecutionEnvironment()
                .fromElements(getObjFunction(modelType, params));

        if (params.contains(LinearTrainParams.OPTIM_METHOD)) {
            LinearTrainParams.OptimMethod method = params.get(LinearTrainParams.OPTIM_METHOD);
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
     * Transform train data to Tuple3 format.
     *
     * @param in        train data in row format.
     * @param params    train parameters.
     * @param isRegProc is regression process or not.
     * @return Tuple3 format train data <weight, label, vector></>.
     */
    public static DataSet<Tuple3<Double, Object, Vector>> transform(BatchOperator in,
                                                                    Params params,
                                                                    boolean isRegProc,
                                                                    boolean calcMeanVar) {
        String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
        String labelName = params.get(LinearTrainParams.LABEL_COL);
        String weightColName = params.get(LinearTrainParams.WEIGHT_COL);
        String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
        final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
        TableSchema dataSchema = in.getSchema();
        if (null == featureColNames && null == vectorColName) {
            featureColNames = TableUtil.getNumericCols(dataSchema, new String[]{labelName});
            params.set(LinearTrainParams.FEATURE_COLS, featureColNames);
        }
        int[] featureIndices = null;
        int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
        if (featureColNames != null) {
            featureIndices = new int[featureColNames.length];
            for (int i = 0; i < featureColNames.length; ++i) {
                int idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), featureColNames[i]);
                featureIndices[i] = idx;
                TypeInformation type = in.getSchema().getFieldTypes()[idx];

                Preconditions.checkState(TableUtil.isSupportedNumericType(type),
                        "linear algorithm only support numerical data type. type is : " + type);
            }
        }
        int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
                weightColName)
                : -1;
        int vecIdx = vectorColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName)
                : -1;

        return in.getDataSet().mapPartition(new Transform(isRegProc, weightIdx,
                vecIdx, featureIndices, labelIdx, hasIntercept, calcMeanVar));
    }

    /**
     * Get feature types.
     *
     * @param in              train data.
     * @param featureColNames feature column names.
     * @return feature types.
     */
    protected static String[] getFeatureTypes(BatchOperator in, String[] featureColNames) {
        if (featureColNames != null) {
            String[] featureColTypes = new String[featureColNames.length];
            for (int i = 0; i < featureColNames.length; ++i) {
                int idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), featureColNames[i]);
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
     * @param initData  initial data.
     * @param params    train parameters.
     * @param isRegProc train process is regression or classification.
     * @param meanVar   mean and variance of train data.
     * @return train data after standardization.
     */
    protected static DataSet<Tuple3<Double, Double, Vector>> preProcess(
            DataSet<Tuple3<Double, Object, Vector>> initData,
            Params params,
            final boolean isRegProc,
            DataSet<DenseVector[]> meanVar,
            DataSet<Object[]> labelValues,
            DataSet<Integer> featSize) {
        // Get parameters.
        final boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);
        final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
        return initData.mapPartition(
                new RichMapPartitionFunction<Tuple3<Double, Object, Vector>, Tuple3<Double, Double, Vector>>() {
                    private static final long serialVersionUID = -3931917328901089041L;
                    private DenseVector[] meanVar;
                    private Object[] labelValues = null;
                    private int featureSize;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.meanVar = (DenseVector[]) getRuntimeContext()
                                .getBroadcastVariable(MEAN_VAR).get(0);
                        this.labelValues = (Object[]) getRuntimeContext()
                                .getBroadcastVariable(LABEL_VALUES).get(0);
                        this.featureSize = (int) getRuntimeContext().getBroadcastVariable("featureSize").get(0);
                        modifyMeanVar(standardization, meanVar);
                    }

                    @Override
                    public void mapPartition(Iterable<Tuple3<Double, Object, Vector>> values,
                                             Collector<Tuple3<Double, Double, Vector>> out) throws Exception {
                        for (Tuple3<Double, Object, Vector> value : values) {
                            Vector aVector = value.f2;

                            if (value.f0 > 0) {
                                Double label = isRegProc ? Double.valueOf(value.f1.toString())
                                        : (value.f1.equals(labelValues[0]) ? 1.0 : -1.0);
                                if (aVector instanceof DenseVector) {
                                    if (aVector.size() < featureSize) {
                                       DenseVector tmp = new DenseVector(featureSize);
                                       for (int i = 0; i < aVector.size(); ++i) {
                                           tmp.set(i, aVector.get(i));
                                       }
                                       aVector = tmp;
                                    }
                                    if (standardization) {
                                        if (hasIntercept) {
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
                                        int[] indices = ((SparseVector) aVector).getIndices();
                                        double[] vals = ((SparseVector) aVector).getValues();
                                        for (int i = 0; i < indices.length; ++i) {
                                            vals[i] = vals[i] / meanVar[1].get(indices[i]);
                                        }
                                    }
                                    if (aVector.size() == -1 || aVector.size() == 0) {
                                        ((SparseVector) aVector).setSize(featureSize);
                                    }
                                }
                                out.collect(Tuple3.of(value.f0, label, aVector));
                            }
                        }
                    }
                }).withBroadcastSet(meanVar, MEAN_VAR)
                .withBroadcastSet(labelValues, LABEL_VALUES)
                .withBroadcastSet(featSize, "featureSize");
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
                Preconditions.checkState((lambda > 0), "lambda must be positive number or zero! lambda is : " +
                        lambda);
                params.set(HasL2.L_2, lambda);
                params.remove(RidgeRegTrainParams.LAMBDA);
            } else if ("LASSO".equals(modelName)) {
                double lambda = params.get(LassoRegTrainParams.LAMBDA);
                if (lambda < 0) {
                    throw new RuntimeException("lambda must be positive number or zero!");
                }
                params.set(HasL1.L_1, lambda);
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

            params.set(HasL2.L_2, 1.0 / cParam);
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
                    for (int i = 1; i < n; ++i) {
                        sum += coefVector.f0.get(i) * meanVar[0].get(i) / meanVar[1].get(i);
                        coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(i));
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
        modelData.convergenceInfo = coefVector.f1;
        modelData.labelName = meta.get(LinearTrainParams.LABEL_COL);
        modelData.featureTypes = meta.get(ModelParamName.FEATURE_TYPES);

        return modelData;
    }

    /**
     * Create meta info.
     */
    public static class CreateMeta implements MapPartitionFunction<Object[], Params> {
        private static final long serialVersionUID = 536971312646228170L;
        private String modelName;
        private LinearModelType modelType;
        private boolean hasInterceptItem;
        private String vectorColName;
        private String labelName;

        public CreateMeta(String modelName, LinearModelType modelType, Params params) {
            this.modelName = modelName;
            this.modelType = modelType;
            this.hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
            this.vectorColName = params.get(LinearTrainParams.VECTOR_COL);
            this.labelName = params.get(LinearTrainParams.LABEL_COL);
        }

        @Override
        public void mapPartition(Iterable<Object[]> rows, Collector<Params> metas) throws Exception {
            Object[] labels = rows.iterator().next();

            Params meta = new Params();
            meta.set(ModelParamName.MODEL_NAME, this.modelName);
            meta.set(ModelParamName.LINEAR_MODEL_TYPE, this.modelType);
            if (LinearModelType.LinearReg != modelType && LinearModelType.SVR != modelType
                    && LinearModelType.AFT != modelType) {
                meta.set(ModelParamName.LABEL_VALUES, labels);
            }
            meta.set(ModelParamName.HAS_INTERCEPT_ITEM, this.hasInterceptItem);
            meta.set(ModelParamName.VECTOR_COL_NAME, vectorColName);
            meta.set(LinearTrainParams.LABEL_COL, labelName);
            metas.collect(meta);
        }
    }

    /**
     * Transform the train data to Tuple3 format: Tuple3<weightValue, labelValue, featureVector>
     */
    private static class Transform extends RichMapPartitionFunction<Row, Tuple3<Double, Object, Vector>> {

        private static final long serialVersionUID = 4360321564414289067L;
        private boolean isRegProc;
        private int weightIdx;
        private int vecIdx;
        private int labelIdx;
        private int[] featureIndices;
        private boolean hasIntercept;
        private boolean calcMeanVar;
        private boolean hasSparseVector = false;
        private boolean hasDenseVector = false;
        private boolean hasNull = false;
        private Map<Integer, double[]> meanVarMap = new HashMap<>();

        public Transform(boolean isRegProc, int weightIdx, int vecIdx,
                         int[] featureIndices, int labelIdx, boolean hasIntercept, boolean calcMeanVar) {
            this.isRegProc = isRegProc;
            this.weightIdx = weightIdx;
            this.vecIdx = vecIdx;
            this.featureIndices = featureIndices;
            this.labelIdx = labelIdx;
            this.hasIntercept = hasIntercept;
            this.calcMeanVar = calcMeanVar;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (hasNull) {
                throw new RuntimeException("the input data has null values, please check it!");
            }

        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Tuple3<Double, Object, Vector>> out)
                throws Exception {

            Set<Object> labelValues = new HashSet<>();
            int size = -1;

            Vector meanVar = null;
            DenseVector tmpMeanVar = null;
            if (featureIndices != null) {
                size = hasIntercept ? featureIndices.length + 1 : featureIndices.length;
                meanVar = calcMeanVar ? new DenseVector(3 * size + 1) : new DenseVector(0);
            } else {
                meanVar = calcMeanVar ? null : new DenseVector(0);
            }

            for (Row row : values) {
                Double weight = weightIdx != -1 ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
                Object val = row.getField(labelIdx);

                if (!this.isRegProc) {
                    labelValues.add(val);
                } else {
                    labelValues.add(0.0);
                }
                if (featureIndices != null) {
                    if (hasIntercept) {
                        DenseVector vec = new DenseVector(featureIndices.length + 1);
                        vec.set(0, 1.0);
                        if (calcMeanVar) {
                            meanVar.add(0, 1.0);
                            meanVar.add(size, 1.0);
                        }
                        for (int i = 1; i < featureIndices.length + 1; ++i) {
                            if (row.getField(featureIndices[i - 1]) == null) {
                                hasNull = true;
                            } else {
                                double fval = ((Number) row.getField(featureIndices[i - 1])).doubleValue();
                                vec.set(i, fval);
                                if (calcMeanVar) {
                                    meanVar.add(i, fval);
                                    meanVar.add(size + i, fval * fval);
                                }
                            }
                        }
                        if (calcMeanVar) {
                            meanVar.add(3 * size, 1.0);
                        }
                        out.collect(Tuple3.of(weight, val, vec));
                    } else {
                        DenseVector vec = new DenseVector(featureIndices.length);
                        for (int i = 0; i < featureIndices.length; ++i) {
                            if (row.getField(featureIndices[i]) == null) {
                                hasNull = true;
                            } else {
                                double fval = ((Number) row.getField(featureIndices[i])).doubleValue();
                                vec.set(i, fval);
                                if (calcMeanVar) {
                                    meanVar.add(i, fval);
                                    meanVar.add(size + i, fval * fval);
                                }
                            }
                        }
                        if (calcMeanVar) {
                            meanVar.add(3 * size, 1.0);
                        }
                        out.collect(Tuple3.of(weight, val, vec));
                    }
                } else {
                    Vector vec = VectorUtil.getVector(row.getField(vecIdx));
                    Preconditions.checkState((vec != null),
                            "vector for linear model train is null, please check your input data.");
                    if (vec instanceof SparseVector) {
                        hasSparseVector = true;
                        if (hasIntercept) {
                            Vector vecNew = vec.prefix(1.0);
                            int[] indices = ((SparseVector) vecNew).getIndices();
                            double[] vals = ((SparseVector) vecNew).getValues();
                            for (int i = 0; i < indices.length; ++i) {
                                size = Math.max(vecNew.size(), Math.max(size, indices[i] + 1));
                                if (calcMeanVar) {
                                    if (meanVarMap.containsKey(indices[i])) {
                                        double[] mv = meanVarMap.get(indices[i]);
                                        mv[0] = Math.max(mv[0], Math.abs(vals[i]));
                                    } else {
                                        meanVarMap.put(indices[i], new double[]{Math.abs(vals[i])});
                                    }
                                }
                            }
                            out.collect(Tuple3.of(weight, val, vecNew));
                        } else {
                            int[] indices = ((SparseVector) vec).getIndices();
                            double[] vals = ((SparseVector) vec).getValues();
                            for (int i = 0; i < indices.length; ++i) {
                                size = Math.max(vec.size(), Math.max(size, indices[i] + 1));
                                if (calcMeanVar) {
                                    if (meanVarMap.containsKey(indices[i])) {
                                        double[] mv = meanVarMap.get(indices[i]);
                                        mv[0] = Math.max(mv[0], Math.abs(vals[i]));
                                    } else {
                                        meanVarMap.put(indices[i], new double[]{Math.abs(vals[i])});
                                    }
                                }
                            }
                            out.collect(Tuple3.of(weight, val, vec));
                        }
                    } else {
                        hasDenseVector = true;
                        if (hasIntercept) {
                            Vector vecNew = vec.prefix(1.0);
                            double[] vals = ((DenseVector) vecNew).getData();
                            size = vals.length;
                            if (calcMeanVar) {
                                if (tmpMeanVar == null) {
                                    tmpMeanVar = new DenseVector(3 * size + 1);
                                }

                                for (int i = 0; i < size; ++i) {
                                    double fval = vecNew.get(i);
                                    tmpMeanVar.add(i, fval);
                                    tmpMeanVar.add(size + i, fval * fval);
                                    tmpMeanVar.set(2 * size + i, Math.max(tmpMeanVar.get(2 * size + i), fval));
                                }
                                tmpMeanVar.add(3 * size, 1.0);
                            }
                            out.collect(Tuple3.of(weight, val, vecNew));
                        } else {
                            double[] vals = ((DenseVector) vec).getData();
                            size = vals.length;
                            if (calcMeanVar) {
                                if (tmpMeanVar == null) {
                                    tmpMeanVar = new DenseVector(3 * size + 1);
                                }
                                for (int i = 0; i < size; ++i) {
                                    double fval = vec.get(i);
                                    tmpMeanVar.add(i, fval);
                                    tmpMeanVar.add(size + i, fval * fval);
                                    tmpMeanVar.set(2 * size + i, Math.max(tmpMeanVar.get(2 * size + i), fval));
                                }
                                tmpMeanVar.add(3 * size, 1.0);
                            }
                            out.collect(Tuple3.of(weight, val, vec));
                        }
                    }
                }
            }
            if (meanVar == null) {
                if (hasSparseVector && (!hasDenseVector)) {
                    meanVar = new DenseVector(size);
                    for (Integer idx : meanVarMap.keySet()) {
                        double[] mv = meanVarMap.get(idx);
                        meanVar.set(idx, mv[0]);
                    }
                } else if (hasSparseVector && hasDenseVector) {
                    meanVar = new DenseVector(size);
                    for (Integer idx : meanVarMap.keySet()) {
                        double[] mv = meanVarMap.get(idx);
                        meanVar.set(idx, mv[0]);
                    }
                    for (int i = 0; i < size; ++i) {
                        meanVar.set(i, Math.max(meanVar.get(i), Math.abs(tmpMeanVar.get(2 * size + i))));
                    }
                } else {
                    meanVar = tmpMeanVar;
                }
            }
            if (hasSparseVector) {
                out.collect(
                        Tuple3.of(-1.0, Tuple2.of(size, labelValues.toArray()), meanVar));
            } else if (hasDenseVector || featureIndices != null) {
                out.collect(
                        Tuple3.of(-2.0, Tuple2.of(size, labelValues.toArray()), meanVar));
            }
        }
    }

    /**
     * build the linear model rows, the format to be output.
     */
    public static class BuildModelFromCoefs extends AbstractRichFunction implements
            MapPartitionFunction<Tuple2<DenseVector, double[]>, Row> {
        private static final long serialVersionUID = -8526938457839413291L;
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
            this.meta = (Params) getRuntimeContext().getBroadcastVariable(META).get(0);
            this.meta.set(ModelParamName.FEATURE_TYPES, featureColTypes);
            if (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE))) {
                this.meanVar = null;
            } else {
                this.meanVar = (DenseVector[]) getRuntimeContext().getBroadcastVariable(MEAN_VAR).get(0);
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

    @Override
    public LinearModelTrainInfo createTrainInfo(List<Row> rows) {
        return new LinearModelTrainInfo(rows);
    }

    @Override
    public BatchOperator<?> getSideOutputTrainInfo() {
        return this.getSideOutput(0);
    }
}