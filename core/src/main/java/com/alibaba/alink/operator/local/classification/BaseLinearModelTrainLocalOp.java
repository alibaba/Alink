package com.alibaba.alink.operator.local.classification;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnimplementedOperationException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.AftRegObjFunc;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.SoftmaxObjFunc;
import com.alibaba.alink.operator.common.linear.UnaryLossObjFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.PerceptronLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SmoothHingeLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SquareLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SvrLossFunc;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class of linear model training. Linear binary classification and linear regression algorithms should inherit
 * this class. Then it only need to write the code of loss function and regular item.
 *
 * @param <T> parameter of this class. Maybe the Svm, linearRegression or Lr parameter.
 */

@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_INFO),
	@PortSpec(value = PortType.DATA, desc = PortDesc.FEATURE_IMPORTANCE),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_WEIGHT)
})

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule

@Internal
public abstract class BaseLinearModelTrainLocalOp<T extends BaseLinearModelTrainLocalOp <T>> extends LocalOperator <T>
	//implements WithTrainInfo <LinearModelTrainInfo, T>
{
	private final String modelName;
	private final LinearModelType linearModelType;
	private static final String META = "meta";
	private static final String MEAN_VAR = "meanVar";
	private static final String LABEL_VALUES = "labelValues";

	/**
	 * @param params    parameters needed by training process.
	 * @param modelType model type: LR, SVR, SVM, Ridge ...
	 * @param modelName name of model.
	 */
	public BaseLinearModelTrainLocalOp(Params params, LinearModelType modelType, String modelName) {
		super(params);
		this.modelName = modelName;
		this.linearModelType = modelType;
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in;
		LocalOperator <?> initModel = null;
		if (inputs.length == 1) {
			in = checkAndGetFirst(inputs);
		} else {
			in = inputs[0];
			initModel = inputs[1];
		}
		/* Get parameters of this algorithm. */
		final Params params = getParams();
		if (params.contains(HasFeatureCols.FEATURE_COLS) && params.contains(HasVectorCol.VECTOR_COL)) {
			throw new AkIllegalArgumentException("FeatureCols and vectorCol cannot be set at the same time.");
		}

		try {
			MTable mt = in.getOutputTable();

			/* Get type of processing: regression or not */
			final boolean isRegProc = getIsRegProc(params, linearModelType, modelName);
			final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
			final TypeInformation <?> labelType = isRegProc ? Types.DOUBLE : mt.getColTypes()[TableUtil
				.findColIndexWithAssertAndHint(mt.getColNames(), params.get(LinearTrainParams.LABEL_COL))];

			// Transform data to Tuple3 format <weight, label, feature vector>. meanAndVar, dim, labelsSort
			Tuple4 <List <Tuple3 <Double, Double, Vector>>, DenseVector[], Integer, Object[]> t4
				= preprocess(mt, params, isRegProc, linearModelType);
			List <Tuple3 <Double, Double, Vector>> trainData = t4.f0;
			DenseVector[] meanVar = t4.f1;
			Integer featSize = t4.f2;
			Object[] labelValues = t4.f3;

			DenseVector initModelCoefs = (null == initModel) ?
				DenseVector.zeros(featSize * (isRegProc ? 1 : (labelValues.length - 1))) :
				initializeModelCoefs(initModel.getOutputTable().getRows(), featSize, meanVar, params, linearModelType);

			if (LinearModelType.Softmax == linearModelType) {
				params.set(ModelParamName.NUM_CLASSES, labelValues.length);
			}
			Tuple2 <DenseVector, Double> modelCoefs
				= LocalOptimizer.optimize(getObjFunction(linearModelType, params), trainData, initModelCoefs, params);

			String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);

			Params meta = new Params();
			meta.set(ModelParamName.MODEL_NAME, this.modelName);
			meta.set(ModelParamName.LINEAR_MODEL_TYPE, this.linearModelType);
			meta.set(ModelParamName.HAS_INTERCEPT_ITEM, hasIntercept);
			meta.set(ModelParamName.VECTOR_COL_NAME, params.get(LinearTrainParams.VECTOR_COL));
			meta.set(LinearTrainParams.LABEL_COL, params.get(LinearTrainParams.LABEL_COL));
			meta.set(ModelParamName.FEATURE_TYPES, getFeatureTypes(mt.getSchema(), featureColNames));
			if (LinearModelType.LinearReg != linearModelType && LinearModelType.SVR != linearModelType
				&& LinearModelType.AFT != linearModelType) {
				meta.set(ModelParamName.LABEL_VALUES, labelValues);
			}
			if (LinearModelType.Softmax == linearModelType) {
				meta.set(ModelParamName.NUM_CLASSES, labelValues.length);
			}

			if (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE))) {
				meanVar = null;
			}
			LinearModelData modelData = buildLinearModelData(meta, featureColNames, labelType, meanVar,
				hasIntercept,
				params.get(LinearTrainParams.STANDARDIZATION),
				Tuple2.of(modelCoefs.f0, new double[] {modelCoefs.f1}));

			LinearModelDataConverter linearModelDataConverter = new LinearModelDataConverter(labelType);
			RowCollector rowCollector = new RowCollector();
			linearModelDataConverter.save(modelData, rowCollector);

			this.setOutputTable(new MTable(rowCollector.getRows(), linearModelDataConverter.getModelSchema()));

			//this.setSideOutputTables(getSideTablesOfCoefficient(modelRows, initData, featSize,
			//	params.get(LinearTrainParams.FEATURE_COLS),
			//	params.get(LinearTrainParams.WITH_INTERCEPT),
			//	getMLEnvironmentId()));
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}

		return (T) this;
	}

	public static DenseVector initializeModelCoefs(List <Row> modelRows,
												   Integer featSize,
												   DenseVector[] meanVar,
												   Params params,
												   final LinearModelType localLinearModelType) {

		LinearModelData model = new LinearModelDataConverter().load(modelRows);

		if (!(model.hasInterceptItem == params.get(HasWithIntercept.WITH_INTERCEPT))) {
			throw new AkIllegalArgumentException("Initial linear model is not compatible with parameter setting."
				+ "InterceptItem parameter setting error.");
		}
		if (!(model.linearModelType == localLinearModelType)) {
			throw new AkIllegalArgumentException("Initial linear model is not compatible with parameter setting."
				+ "linearModelType setting error.");
		}
		if (!(model.vectorSize == featSize)) {
			throw new AkIllegalDataException("Initial linear model is not compatible with training data. "
				+ " vector size not equal, vector size in init model is : " + model.vectorSize +
				" and vector size of train data is : " + featSize);
		}
		int n = meanVar[0].size();
		if (model.hasInterceptItem) {
			double sum = 0.0;
			for (int i = 1; i < n; ++i) {
				sum += model.coefVector.get(i) * meanVar[0].get(i);
				model.coefVector.set(i, model.coefVector.get(i) * meanVar[1].get(i));
			}
			model.coefVector.set(0, model.coefVector.get(0) + sum);
		} else {
			for (int i = 0; i < n; ++i) {
				model.coefVector.set(i, model.coefVector.get(i) * meanVar[1].get(i));
			}
		}

		return model.coefVector;
	}

	public static Table[] getSideTablesOfCoefficient(DataSet <Row> modelRow,
													 DataSet <Tuple3 <Double, Object, Vector>> inputData,
													 DataSet <Integer> vecSize,
													 final String[] featureNames,
													 final boolean hasInterception,
													 long environmentId) {
		DataSet <LinearModelData> model = modelRow.mapPartition(new MapPartitionFunction <Row, LinearModelData>() {
			private static final long serialVersionUID = 2063366042018382802L;

			@Override
			public void mapPartition(Iterable <Row> values, Collector <LinearModelData> out) {
				List <Row> rows = new ArrayList <>();
				for (Row row : values) {
					rows.add(row);
				}
				out.collect(new LinearModelDataConverter().load(rows));
			}
		}).setParallelism(1);

		DataSet <Tuple5 <String, String[], double[], double[], double[]>> allInfo = inputData
			.mapPartition(
				new RichMapPartitionFunction <Tuple3 <Double, Object, Vector>, Tuple3 <Integer, double[], double[]>>
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
					public void mapPartition(Iterable <Tuple3 <Double, Object, Vector>> values,
											 Collector <Tuple3 <Integer, double[], double[]>> out) {
						int iter = 0;
						double[] mu = new double[vectorSize];
						double[] mu2 = new double[vectorSize];
						if (featureNames == null) {
							for (Tuple3 <Double, Object, Vector> t3 : values) {
								if (t3.f0 < 0.0) {
									continue;
								}
								if (t3.f2 instanceof SparseVector) {
									SparseVector tmp = (SparseVector) t3.f2;
									tmp.setSize(vectorSize);

									double[] vecValues = tmp.getValues();
									int[] idx = tmp.getIndices();
									for (int i = 0; i < vecValues.length; ++i) {
										if (hasInterception) {
											if (idx[i] > 0) {
												mu[idx[i] - 1] += vecValues[i];
												mu2[idx[i] - 1] += vecValues[i] * vecValues[i];
											}
										} else {
											mu[idx[i]] += vecValues[i];
											mu2[idx[i]] += vecValues[i] * vecValues[i];
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
							for (Tuple3 <Double, Object, Vector> t3 : values) {
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
			.reduce(new ReduceFunction <Tuple3 <Integer, double[], double[]>>() {
				private static final long serialVersionUID = 7062783877162095989L;

				@Override
				public Tuple3 <Integer, double[], double[]> reduce(Tuple3 <Integer, double[], double[]> t1,
																   Tuple3 <Integer, double[], double[]> t2) {
					t2.f0 = t1.f0 + t2.f0;
					for (int i = 0; i < t1.f1.length; ++i) {
						t2.f1[i] = t1.f1[i] + t2.f1[i];
						t2.f2[i] = t1.f2[i] + t2.f2[i];
					}

					return t2;
				}
			})
			.flatMap(new RichFlatMapFunction <Tuple3 <Integer, double[], double[]>,
				Tuple5 <String, String[], double[], double[], double[]>>() {
				private static final long serialVersionUID = 7815111101106759520L;
				private DenseVector coefVec;
				private LinearModelData model;
				private double[] cinfo;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					model = ((LinearModelData) getRuntimeContext().getBroadcastVariable("model").get(0));
					coefVec = model.coefVector;
					cinfo = model.convergenceInfo;
				}

				@Override
				public void flatMap(Tuple3 <Integer, double[], double[]> value,
									Collector <Tuple5 <String, String[], double[], double[], double[]>> out) {
					double[] importance;
					String[] colNames;
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

		DataSet <Row> importance = allInfo.mapPartition(
			new MapPartitionFunction <Tuple5 <String, String[], double[], double[], double[]>, Row>() {
				private static final long serialVersionUID = -3263497114974298286L;

				@Override
				public void mapPartition(Iterable <Tuple5 <String, String[], double[], double[], double[]>> tuple5s,
										 Collector <Row> out) {

					String[] colNames = null;
					double[] importanceVals = null;
					for (Tuple5 <String, String[], double[], double[], double[]> r : tuple5s) {
						colNames = r.f1;
						importanceVals = r.f3;
					}

					for (int i = 0; i < Objects.requireNonNull(colNames).length; ++i) {
						out.collect(Row.of(colNames[i], importanceVals[i]));
					}
				}
			});
		DataSet <Row> weights = allInfo.mapPartition(
			new MapPartitionFunction <Tuple5 <String, String[], double[], double[], double[]>, Row>() {
				private static final long serialVersionUID = -6164289179429722407L;

				@Override
				public void mapPartition(Iterable <Tuple5 <String, String[], double[], double[], double[]>> tuple5s,
										 Collector <Row> out) {
					String[] colNames = null;
					double[] weights = null;
					for (Tuple5 <String, String[], double[], double[], double[]> r : tuple5s) {
						colNames = r.f1;
						weights = r.f2;
					}
					assert weights != null;
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

		DataSet <Row> summary = allInfo.mapPartition(
			new MapPartitionFunction <Tuple5 <String, String[], double[], double[], double[]>, Row>() {
				private static final long serialVersionUID = -6164289179429722407L;
				private final static int NUM_COLLECT_THRESHOLD = 10000;

				@Override
				public void mapPartition(Iterable <Tuple5 <String, String[], double[], double[], double[]>> tuple5s,
										 Collector <Row> out) {

					for (Tuple5 <String, String[], double[], double[], double[]> r : tuple5s) {
						if (r.f1.length < NUM_COLLECT_THRESHOLD) {
							out.collect(Row.of(0L, r.f0));
							out.collect(Row.of(1L, JsonConverter.toJson(r.f1)));
							out.collect(Row.of(2L, JsonConverter.toJson(r.f2)));
							out.collect(Row.of(3L, JsonConverter.toJson(r.f3)));
							out.collect(Row.of(4L, JsonConverter.toJson(r.f4)));
						} else {
							List <Tuple3 <String, Double, Double>> array = new ArrayList <>(r.f1.length);
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

							out.collect(Row.of(0L, r.f0));
							out.collect(Row.of(1L, JsonConverter.toJson(colName)));
							out.collect(Row.of(2L, JsonConverter.toJson(weight)));
							out.collect(Row.of(3L, JsonConverter.toJson(importance)));
							out.collect(Row.of(4L, JsonConverter.toJson(r.f4)));
						}
					}
				}
			});

		Table summaryTable = DataSetConversionUtil.toTable(environmentId, summary, new TableSchema(
			new String[] {"id", "info"}, new TypeInformation[] {Types.LONG, Types.STRING}));
		Table importanceTable = DataSetConversionUtil.toTable(environmentId, importance, new TableSchema(
			new String[] {"col_name", "importance"}, new TypeInformation[] {Types.STRING, Types.DOUBLE}));
		Table weightTable = DataSetConversionUtil.toTable(environmentId, weights, new TableSchema(
			new String[] {"col_name", "weight"}, new TypeInformation[] {Types.STRING, Types.DOUBLE}));
		return new Table[] {summaryTable, importanceTable, weightTable};
	}

	public static Comparator <Tuple3 <String, Double, Double>> compare = (o1, o2) -> o2.f2.compareTo(o1.f2);

	/**
	 * Order by the dictionary order, only classification problem need do this process.
	 *
	 * @param unorderedLabelRows Unordered label rows.
	 * @return Ordered label rows.
	 */
	private static Object[] orderLabels(Iterable <Object> unorderedLabelRows) {
		List <Object> tmpArr = new ArrayList <>();
		for (Object row : unorderedLabelRows) {
			tmpArr.add(row);
		}
		Object[] labels = tmpArr.toArray(new Object[0]);
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
	 * Get obj function.
	 *
	 * @param modelType Model type.
	 * @param params    Parameters for train.
	 * @return Obj function.
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
			case Softmax:
				objFunc = new SoftmaxObjFunc(params);
				break;
			default:
				throw new AkUnimplementedOperationException("Linear model type is Not implemented yet!");
		}
		return objFunc;
	}

	///**
	// * Transform train data to Tuple3 format.
	// *
	// * @param in        train data in row format.
	// * @param params    train parameters.
	// * @param isRegProc is regression process or not.
	// * @return Tuple3 format train data <weight, label, vector></>.
	// */
	//public static DataSet <Tuple3 <Double, Object, Vector>> transform(BatchOperator <?> in,
	//																  Params params,
	//																  boolean isRegProc) {
	//	final boolean calcMeanVar = params.get(LinearTrainParams.STANDARDIZATION);
	//
	//	String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
	//	String labelName = params.get(LinearTrainParams.LABEL_COL);
	//	String weightColName = params.get(LinearTrainParams.WEIGHT_COL);
	//	String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
	//	final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
	//	TableSchema dataSchema = in.getSchema();
	//	if (null == featureColNames && null == vectorColName) {
	//		featureColNames = TableUtil.getNumericCols(dataSchema, new String[] {labelName});
	//		params.set(LinearTrainParams.FEATURE_COLS, featureColNames);
	//	}
	//	int[] featureIndices = null;
	//	int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
	//	if (featureColNames != null) {
	//		featureIndices = new int[featureColNames.length];
	//		for (int i = 0; i < featureColNames.length; ++i) {
	//			int idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), featureColNames[i]);
	//			featureIndices[i] = idx;
	//			TypeInformation <?> type = in.getSchema().getFieldTypes()[idx];
	//
	//			AkPreconditions.checkState(TableUtil.isSupportedNumericType(type),
	//				"linear algorithm only support numerical data type. Current type is : " + type);
	//		}
	//	}
	//	int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
	//		weightColName) : -1;
	//	int vecIdx = vectorColName != null ?
	//		TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName) : -1;
	//
	//	return in.getDataSet().mapPartition(new Transform(isRegProc, weightIdx,
	//		vecIdx, featureIndices, labelIdx, hasIntercept, calcMeanVar));
	//}

	public static Tuple4 <List <Tuple3 <Double, Double, Vector>>, DenseVector[], Integer, Object[]> preprocess(
		MTable mt, Params params, boolean isRegProc, LinearModelType linearModelType) {

		final boolean calcMeanVar = params.get(LinearTrainParams.STANDARDIZATION);
		final boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);
		final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
		String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
		String labelName = params.get(LinearTrainParams.LABEL_COL);
		String weightColName = params.get(LinearTrainParams.WEIGHT_COL);
		String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
		TableSchema dataSchema = mt.getSchema();
		if (null == featureColNames && null == vectorColName) {
			featureColNames = TableUtil.getNumericCols(dataSchema, new String[] {labelName});
			params.set(LinearTrainParams.FEATURE_COLS, featureColNames);
		}
		int[] featureIndices = null;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
		if (featureColNames != null) {
			featureIndices = new int[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(mt.getColNames(), featureColNames[i]);
				featureIndices[i] = idx;
				TypeInformation <?> type = mt.getSchema().getFieldTypes()[idx];

				AkPreconditions.checkState(TableUtil.isSupportedNumericType(type),
					"linear algorithm only support numerical data type. Current type is : " + type);
			}
		}
		int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(mt.getColNames(),
			weightColName) : -1;
		int vecIdx = vectorColName != null ?
			TableUtil.findColIndexWithAssertAndHint(mt.getColNames(), vectorColName) : -1;

		List <Tuple3 <Double, Object, Vector>> resultList = new ArrayList <>();
		Set <Object> labelValues = new HashSet <>();

		boolean hasSparseVector = false;
		boolean hasDenseVector = false;
		boolean hasNull = false;
		boolean hasLabelNull = false;

		int featureSize = -1;
		int cnt = mt.getNumRow();
		Vector meanVar;
		DenseVector tmpMeanVar = null;
		Map <Integer, double[]> meanVarMap = new HashMap <>();

		if (featureIndices != null) {
			featureSize = hasIntercept ? featureIndices.length + 1 : featureIndices.length;
			meanVar = calcMeanVar ? new DenseVector(3 * featureSize + 1) : new DenseVector(1);
		} else {
			meanVar = calcMeanVar ? null : new DenseVector(1);
		}

		for (Row row : mt.getRows()) {
			Double weight = weightIdx != -1 ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
			Object val = row.getField(labelIdx);

			if (!isRegProc) {
				labelValues.add(val);
			}

			if (null == val) {
				hasLabelNull = true;
			}

			if (featureIndices != null) {
				if (hasIntercept) {
					DenseVector vec = new DenseVector(featureIndices.length + 1);
					vec.set(0, 1.0);
					if (calcMeanVar) {
						meanVar.add(0, 1.0);
						meanVar.add(featureSize, 1.0);
					}
					for (int i = 1; i < featureIndices.length + 1; ++i) {
						if (row.getField(featureIndices[i - 1]) == null) {
							hasNull = true;
						} else {
							double fVal = ((Number) row.getField(featureIndices[i - 1])).doubleValue();
							vec.set(i, fVal);
							if (calcMeanVar) {
								meanVar.add(i, fVal);
								meanVar.add(featureSize + i, fVal * fVal);
							}
						}
					}
					if (calcMeanVar) {
						meanVar.add(3 * featureSize, 1.0);
					}
					resultList.add(Tuple3.of(weight, val, vec));
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
								meanVar.add(featureSize + i, fval * fval);
							}
						}
					}
					if (calcMeanVar) {
						meanVar.add(3 * featureSize, 1.0);
					}
					resultList.add(Tuple3.of(weight, val, vec));
				}
			} else {
				Vector vec = VectorUtil.getVector(row.getField(vecIdx));
				AkPreconditions.checkState((vec != null),
					"Vector for linear model train is null, please check your input data.");
				if (vec instanceof SparseVector) {
					hasSparseVector = true;
					//if (hasIntercept) {
					//	Vector vecNew = vec.prefix(1.0);
					//	int[] indices = ((SparseVector) vecNew).getIndices();
					//	double[] vals = ((SparseVector) vecNew).getValues();
					//	for (int i = 0; i < indices.length; ++i) {
					//		featureSize = Math.max(vecNew.size(), Math.max(featureSize, indices[i] + 1));
					//		if (calcMeanVar) {
					//			if (meanVarMap.containsKey(indices[i])) {
					//				double[] mv = meanVarMap.get(indices[i]);
					//				mv[0] = Math.max(mv[0], Math.abs(vals[i]));
					//			} else {
					//				meanVarMap.put(indices[i], new double[] {Math.abs(vals[i])});
					//			}
					//		}
					//	}
					//	resultList.add(Tuple3.of(weight, val, vecNew));
					//} else {
					//	int[] indices = ((SparseVector) vec).getIndices();
					//	double[] vals = ((SparseVector) vec).getValues();
					//	for (int i = 0; i < indices.length; ++i) {
					//		featureSize = Math.max(vec.size(), Math.max(featureSize, indices[i] + 1));
					//		if (calcMeanVar) {
					//			if (meanVarMap.containsKey(indices[i])) {
					//				double[] mv = meanVarMap.get(indices[i]);
					//				mv[0] = Math.max(mv[0], Math.abs(vals[i]));
					//			} else {
					//				meanVarMap.put(indices[i], new double[] {Math.abs(vals[i])});
					//			}
					//		}
					//	}
					//	resultList.add(Tuple3.of(weight, val, vec));
					//}
					Vector vecNew = hasIntercept ? vec.prefix(1.0) : vec;
					int[] indices = ((SparseVector) vecNew).getIndices();
					double[] vals = ((SparseVector) vecNew).getValues();
					for (int i = 0; i < indices.length; ++i) {
						featureSize = Math.max(vecNew.size(), featureSize);
						if (calcMeanVar) {
							if (meanVarMap.containsKey(indices[i])) {
								double[] mv = meanVarMap.get(indices[i]);
								mv[0] += vals[i];
								mv[1] += vals[i] * vals[i];
							} else {
								meanVarMap.put(indices[i], new double[] {vals[i], vals[i] * vals[i]});
							}
						}
					}
					resultList.add(Tuple3.of(weight, val, vecNew));

				} else {
					hasDenseVector = true;
					if (hasIntercept) {
						Vector vecNew = vec.prefix(1.0);
						double[] vals = ((DenseVector) vecNew).getData();
						featureSize = vals.length;
						if (calcMeanVar) {
							if (tmpMeanVar == null) {
								tmpMeanVar = new DenseVector(3 * featureSize + 1);
							}

							for (int i = 0; i < featureSize; ++i) {
								double fval = vecNew.get(i);
								tmpMeanVar.add(i, fval);
								tmpMeanVar.add(featureSize + i, fval * fval);
								tmpMeanVar.set(2 * featureSize + i,
									Math.max(tmpMeanVar.get(2 * featureSize + i), fval));
							}
							tmpMeanVar.add(3 * featureSize, 1.0);
						}
						resultList.add(Tuple3.of(weight, val, vecNew));
					} else {
						double[] vals = ((DenseVector) vec).getData();
						featureSize = vals.length;
						if (calcMeanVar) {
							if (tmpMeanVar == null) {
								tmpMeanVar = new DenseVector(3 * featureSize + 1);
							}
							for (int i = 0; i < featureSize; ++i) {
								double fval = vec.get(i);
								tmpMeanVar.add(i, fval);
								tmpMeanVar.add(featureSize + i, fval * fval);
								tmpMeanVar.set(2 * featureSize + i,
									Math.max(tmpMeanVar.get(2 * featureSize + i), fval));
							}
							tmpMeanVar.add(3 * featureSize, 1.0);
						}
						resultList.add(Tuple3.of(weight, val, vec));
					}
				}
			}
		}

		if (hasNull) {
			throw new AkIllegalDataException("The input data has null values, please check it!");
		}
		if (hasLabelNull) {
			throw new AkIllegalDataException("The input labels has null values, please check it!");
		}

		if (meanVar == null) {
			if (hasSparseVector && (!hasDenseVector)) {
				meanVar = new DenseVector(featureSize * 2);
				for (Integer idx : meanVarMap.keySet()) {
					double[] mv = meanVarMap.get(idx);
					meanVar.set(idx, mv[0]);
					meanVar.set(featureSize + idx, mv[1]);
				}
			} else if (hasSparseVector) {
				meanVar = new DenseVector(featureSize + 1);
				for (Integer idx : meanVarMap.keySet()) {
					double[] mv = meanVarMap.get(idx);
					meanVar.set(idx, mv[0]);
				}
				for (int i = 0; i < featureSize; ++i) {
					meanVar.set(i, Math.max(meanVar.get(i), Math.abs(tmpMeanVar.get(2 * featureSize + i))));
				}
			} else {
				meanVar = tmpMeanVar;
			}
		}

		final int maxLabels = 1000;
		final double labelRatio = 0.5;
		if (labelValues.size() > mt.getNumRow() * labelRatio && labelValues.size() > maxLabels) {
			throw new AkIllegalDataException("label num is : " + labelValues.size() + ","
				+ " sample num is : " + mt.getNumRow() + ", please check your label column.");
		}
		Object[] labelsSort = isRegProc ? labelValues.toArray() : orderLabels(labelValues);

		HashMap <Object, Double> labelMap = new HashMap <>();
		if (!isRegProc) {
			if (LinearModelType.Softmax == linearModelType) {
				for (int i = 0; i < labelsSort.length; i++) {
					labelMap.put(labelsSort[i], Double.valueOf(i));
				}
			} else {
				if (labelsSort.length == 2) {
					labelMap.put(labelsSort[0], 1.0);
					labelMap.put(labelsSort[1], -1.0);
				} else {
					StringBuilder sbd = new StringBuilder();
					for (int i = 0; i < Math.min(labelsSort.length, 10); i++) {
						sbd.append(labelsSort[i]);
						if (i > 0) {sbd.append(",");}
					}
					if (labelsSort.length > 10) {
						sbd.append(", ...... ");
					}
					throw new AkIllegalDataException(
						linearModelType + " need 2 label values, but training data's distinct label values : " + sbd);
				}
			}
		}

		DenseVector[] meanAndVar = new DenseVector[2];
		meanAndVar[0] = calcMeanVar ? new DenseVector(featureSize) : new DenseVector(0);
		meanAndVar[1] = calcMeanVar ? new DenseVector(featureSize) : new DenseVector(0);

		if (calcMeanVar) {
			double mean;
			double stdvar;
			for (int i = 0; i < featureSize; ++i) {
				mean = meanVar.get(i) / cnt;
				meanAndVar[0].set(i, mean);
				stdvar = Math.sqrt(Math.max(0.0, meanVar.get(featureSize + i) - cnt * mean * mean) / (cnt - 1));
				meanAndVar[1].set(i, stdvar);
			}
			modifyMeanVar(calcMeanVar, meanAndVar);
		}

		List <Tuple3 <Double, Double, Vector>> finalList = new ArrayList <>();
		for (Tuple3 <Double, Object, Vector> value : resultList) {
			Vector aVector = value.f2;

			Double label = isRegProc ? Double.parseDouble(value.f1.toString()) : labelMap.get(value.f1);

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
							aVector.set(i,
								(aVector.get(i) - meanAndVar[0].get(i)) / meanAndVar[1].get(i));
						}
					} else {
						for (int i = 0; i < aVector.size(); ++i) {
							aVector.set(i, aVector.get(i) / meanAndVar[1].get(i));
						}
					}
				}
			} else {
				if (standardization) {
					int[] indices = ((SparseVector) aVector).getIndices();
					double[] vals = ((SparseVector) aVector).getValues();
					for (int i = 0; i < indices.length; ++i) {
						vals[i] = vals[i] / meanAndVar[1].get(indices[i]);
					}
				}
				if (aVector.size() == -1 || aVector.size() == 0) {
					((SparseVector) aVector).setSize(featureSize);
				}
			}

			finalList.add(Tuple3.of(value.f0, label, aVector));
		}

		return Tuple4.of(finalList, meanAndVar, featureSize, labelsSort);

	}

	/**
	 * Get feature types.
	 *
	 * @param in              train data.
	 * @param featureColNames feature column names.
	 * @return feature types.
	 */
	protected static String[] getFeatureTypes(BatchOperator <?> in, String[] featureColNames) {
		return getFeatureTypes(in.getSchema(), featureColNames);
	}

	protected static String[] getFeatureTypes(TableSchema schema, String[] featureColNames) {
		if (featureColNames != null) {
			String[] featureColTypes = new String[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(schema.getFieldNames(), featureColNames[i]);
				TypeInformation <?> type = schema.getFieldTypes()[idx];
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
					throw new AkIllegalArgumentException(
						"Linear algorithm only support numerical data type. Current type is : " + type);
				}
			}
			return featureColTypes;
		}
		return null;
	}

	/**
	 * In this function, we do some parameters transformation, just like lambda, tau, and return the type of training:
	 * regression or classification.
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
				AkPreconditions.checkState((lambda > 0), "Lambda must be positive number or zero! lambda is : " +
					lambda);
				params.set(HasL2.L_2, lambda);
				params.remove(RidgeRegTrainParams.LAMBDA);
			} else if ("LASSO".equals(modelName)) {
				double lambda = params.get(LassoRegTrainParams.LAMBDA);
				if (lambda < 0) {
					throw new AkIllegalArgumentException("Lambda must be positive number or zero!");
				}
				params.set(HasL1.L_1, lambda);
				params.remove(RidgeRegTrainParams.LAMBDA);
			}
			return true;
		} else if (linearModelType.equals(LinearModelType.SVR)) {
			Double tau = params.get(LinearSvrTrainParams.TAU);
			double cParam = params.get(LinearSvrTrainParams.C);
			if (tau < 0) {
				throw new AkIllegalArgumentException("Parameter tau must be positive number or zero!");
			}
			if (cParam <= 0) {
				throw new AkIllegalArgumentException("Parameter C must be positive number!");
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
													   TypeInformation <?> labelType,
													   DenseVector[] meanVar,
													   boolean hasIntercept,
													   boolean standardization,
													   Tuple2 <DenseVector, double[]> coefVector) {
		int k1 = 1;
		if (LinearModelType.Softmax == meta.get(ModelParamName.LINEAR_MODEL_TYPE)) {
			k1 = meta.get(ModelParamName.NUM_CLASSES) - 1;
		}

		if (!(LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)))) {
			modifyMeanVar(standardization, meanVar);
		}
		meta.set(ModelParamName.VECTOR_SIZE, coefVector.f0.size() / k1
			- (meta.get(ModelParamName.HAS_INTERCEPT_ITEM) ? 1 : 0)
			- (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)) ? 1 : 0));
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
		//if (1 < k1) {
		//	modelData.coefVectors = new DenseVector[k1];
		//	double[] data = coefVector.f0.getData();
		//	int dim = coefVector.f0.size() / k1;
		//	for (int i = 0; i < k1; i++) {
		//		double[] buf = new double[dim];
		//		for (int j = 0; j < dim; j++) {
		//			buf[j] = data[dim * i + j];
		//		}
		//		modelData.coefVectors[i] = new DenseVector(buf);
		//	}
		//}
		return modelData;
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

	//@Override
	//public LinearModelTrainInfo createTrainInfo(List <Row> rows) {
	//	return new LinearModelTrainInfo(rows);
	//}
	//
	//@Override
	//public BatchOperator <?> getSideOutputTrainInfo() {
	//	return this.getSideOutput(0);
	//}
}