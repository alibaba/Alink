package com.alibaba.alink.operator.common.linear;

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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.common.viz.VizDataWriterForModelInfo;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.finance.ScorecardTrainBatchOp;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SquareLossFunc;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.operator.common.optim.Lbfgs;
import com.alibaba.alink.operator.common.optim.Newton;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintObjFunc;
import com.alibaba.alink.operator.common.optim.activeSet.Sqp;
import com.alibaba.alink.operator.common.optim.barrierIcq.LogBarrier;
import com.alibaba.alink.operator.common.optim.divergence.Alm;
import com.alibaba.alink.operator.common.optim.local.ConstrainedLocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SparseVectorSummary;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.finance.ConstrainedLinearModelParams;
import com.alibaba.alink.params.finance.ConstrainedLogisticRegressionTrainParams;
import com.alibaba.alink.params.finance.HasConstrainedOptimizationMethod;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class of linear model training. Linear binary classification and linear regression algorithms should inherit
 * this class. Then it only need to write the code of loss function and regular item.
 * <p>
 * constraint can be set by params or second input op. if has constraint, standardization is false.
 * <p>
 * if in scorecard or lr, positive value is required.
 * <p>
 * default optim is sqp whether has constraint or not.
 *
 * @param <T> parameter of this class. Maybe the linearRegression or Lr parameter.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.MODEL)
})

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule
public abstract class BaseConstrainedLinearModelTrainBatchOp<T extends BaseConstrainedLinearModelTrainBatchOp <T>>
	extends BatchOperator <T>
	implements AlinkViz <T> {
	private static final long serialVersionUID = 1180583968098354917L;
	private final String modelName;
	private final LinearModelType linearModelType;
	//for viz, when feature num > NUM_FEATURE_THRESHOLD, not viz
	private static final int NUM_FEATURE_THRESHOLD = 10000;
	private static final String META = "meta";
	private static final String MEAN_VAR = "meanVar";
	//inner variable for broadcast.
	private static final String VECTOR_SIZE = "vectorSize";
	private static final String LABEL_VALUES = "labelValues";

	/**
	 * @param params    parameters needed by training process.
	 * @param modelType model type: LR, LinearReg
	 * @param modelName name of model.
	 */
	public BaseConstrainedLinearModelTrainBatchOp(Params params, LinearModelType modelType, String modelName) {
		super(params);
		this.modelName = modelName;
		this.linearModelType = modelType;
	}

	/**
	 * @param inputs first is data, second is constraint. first is required and second is optioned. constraint can from
	 *               second input or params.
	 */
	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		Params params = getParams();

		BatchOperator <?> in = inputs[0];
		DataSet <Row> constraints = null;

		//when has constraint, STANDARDIZATION is false.
		if (inputs.length == 2 && inputs[1] != null) {
			constraints = inputs[1].getDataSet();
			params.set(LinearTrainParams.STANDARDIZATION, false);
		}

		//constraint can from second input or params.
		String cons = params.get(ConstrainedLinearModelParams.CONSTRAINT);

		//in scorecard, it is always "" not null.
		if (!"".equals(cons)) {
			constraints = MLEnvironmentFactory
				.get(this.getMLEnvironmentId()).getExecutionEnvironment().fromElements(
					Row.of(FeatureConstraint.fromJson(cons)));
			params.set(LinearTrainParams.STANDARDIZATION, false);
		} else {
			if (constraints != null) {
				constraints = inputs[1].getDataSet();
				//here deal with input constraint which may be string (from constrained linear model)
				constraints = constraints.map(new GenerateConstraint());
			} else {
				constraints = MLEnvironmentFactory
					.get(this.getMLEnvironmentId()).getExecutionEnvironment().fromElements(
						Row.of(new FeatureConstraint()));
			}
		}
		String positiveLabel = null;
		//three cases: lr, linearreg in scorecard, linearreg.
		boolean useLabel = LinearModelType.LR == linearModelType || getParams().get(ScorecardTrainBatchOp
			.IN_SCORECARD);
		if (useLabel) {
			positiveLabel = params.get(ConstrainedLogisticRegressionTrainParams.POS_LABEL_VAL_STR);
		}

		//may set true when not have constraint.
		if (!params.contains(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD)) {
			params.set(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD,
				HasConstrainedOptimizationMethod.ConstOptimMethod.SQP);
		}
		String method = params.get(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD).toString().toUpperCase();

		// Get type of processing: regression or not
		boolean isRegProc = linearModelType == LinearModelType.LinearReg;
		boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);

		// Get label info: including label values and label type.
		//If linear regression not in scorecard, label values is new Object().
		Tuple2 <DataSet <Object>, TypeInformation> labelInfo = getLabelInfo(in, params, !useLabel);

		//Tuple3 format train data <weight, label, vector>
		DataSet <Tuple3 <Double, Object, Vector>> initData = BaseLinearModelTrainBatchOp
			.transform(in, params, isRegProc, standardization);

		//Tuple3 <meanAndVar, labelsSort, featureSize>
		DataSet <Tuple3 <DenseVector[], Object[], Integer[]>> utilInfo = BaseLinearModelTrainBatchOp
			.getUtilInfo(initData, standardization, isRegProc);

		//get means and variances
		DataSet <DenseVector[]> meanVar = utilInfo.map(
			new MapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, DenseVector[]>() {
				private static final long serialVersionUID = 7127767376687624403L;

				@Override
				public DenseVector[] map(Tuple3 <DenseVector[], Object[], Integer[]> value) {
					return value.f0;
				}
			});

		//get feature size
		DataSet <Integer> featSize = utilInfo.map(
			new MapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, Integer>() {
				private static final long serialVersionUID = 2773811388068064638L;

				@Override
				public Integer map(Tuple3 <DenseVector[], Object[], Integer[]> value) {
					return value.f2[0];
				}
			});

		//change orders of labels, first value is positiveLabel.
		DataSet <Object[]> labelValues = utilInfo.flatMap(new BuildLabels(isRegProc, positiveLabel));

		//<weight, label, vector>
		// vector is after standard if standardization is true.
		// label is change to 1.0/0.0, if lr or linearReg in scoreCard.
		DataSet <Tuple3 <Double, Double, Vector>> trainData
			= BaseLinearModelTrainBatchOp
			.preProcess(initData, params, isRegProc, meanVar, labelValues, featSize);

		//stat non zero of all features.
		DataSet <DenseVector> countZero = StatisticsHelper.summary(trainData.map(
				new MapFunction <Tuple3 <Double, Double, Vector>, Vector>() {
					private static final long serialVersionUID = 6207307350053531656L;

					@Override
					public Vector map(Tuple3 <Double, Double, Vector> value) {
						return value.f2;
					}
				}).withForwardedFields())
			.map(new MapFunction <BaseVectorSummary, DenseVector>() {
				private static final long serialVersionUID = 2322849507320367330L;

				@Override
				public DenseVector map(BaseVectorSummary value) {
					if (value instanceof SparseVectorSummary) {
						return (DenseVector) ((SparseVectorSummary) value).numNonZero();
					}
					return new DenseVector(0);
				}
			});

		// Solve the optimization problem.
		// return <OptimObjFunc, coefficientDim>
		Tuple2 <DataSet <OptimObjFunc>, DataSet <Integer>> optParam = getOptParam(constraints, params, featSize,
			linearModelType, MLEnvironmentFactory.get(this.getMLEnvironmentId()), method, countZero);
		DataSet <Tuple2 <DenseVector, double[]>> coefVectorSet = optimize(params, optParam, trainData, modelName,
			method);

		// Prepare the meta info of linear model.
		DataSet <Params> meta = labelInfo.f0
			.mapPartition(new CreateMeta(modelName, linearModelType, params, useLabel, positiveLabel))
			.setParallelism(1);

		// Build linear model rows, the format to be output.
		DataSet <Row> modelRows;
		String[] featureColTypes = BaseLinearModelTrainBatchOp.getFeatureTypes(in,
			params.get(LinearTrainParams.FEATURE_COLS));
		modelRows = coefVectorSet
			.mapPartition(new BaseLinearModelTrainBatchOp.BuildModelFromCoefs(labelInfo.f1,
				params.get(LinearTrainParams.FEATURE_COLS),
				params.get(LinearTrainParams.STANDARDIZATION),
				params.get(LinearTrainParams.WITH_INTERCEPT),
				featureColTypes))
			.withBroadcastSet(meta, META)
			.withBroadcastSet(meanVar, MEAN_VAR)
			.setParallelism(1);
		// Convert the model rows to table.
		this.setOutput(modelRows, new LinearModelDataConverter(labelInfo.f1).getModelSchema());
		writeVizData(modelRows, featSize);
		return (T) this;
	}

	private static class GenerateConstraint implements MapFunction <Row, Row> {
		private static final long serialVersionUID = -6999309059934707482L;

		@Override
		public Row map(Row value) {
			FeatureConstraint cons;
			if (value.getField(0) instanceof FeatureConstraint) {
				cons = (FeatureConstraint) value.getField(0);
			} else {
				cons = FeatureConstraint.fromJson((String) value.getField(0));
			}
			return Row.of(cons);
		}
	}

	protected static DataSet <Tuple3 <Double, Double, Vector>> transform(BatchOperator in, Params params,
																		 DataSet <Object> labelValues,
																		 boolean isRegProc, String posLabel,
																		 TypeInformation labelType) {
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
		return in.getDataSet().map(
				new Transform(isRegProc, weightIdx, vecIdx, featureIndices, labelIdx, posLabel, labelType))
			.withBroadcastSet(labelValues, LABEL_VALUES);
	}

	private static class Transform extends RichMapFunction <Row, Tuple3 <Double, Double, Vector>> {
		private static final long serialVersionUID = 3541655329500762922L;
		private final String positiveLableValueString;

		private final boolean isRegProc;
		private final int weightIdx;
		private final int vecIdx;
		private final int labelIdx;
		private final int[] featureIndices;
		private final TypeInformation type;

		public Transform(boolean isRegProc, int weightIdx, int vecIdx, int[] featureIndices,
						 int labelIdx, String posLabel, TypeInformation type) {
			this.isRegProc = isRegProc;
			this.weightIdx = weightIdx;
			this.vecIdx = vecIdx;
			this.featureIndices = featureIndices;
			this.labelIdx = labelIdx;
			this.positiveLableValueString = posLabel;
			this.type = type;
		}

		@Override
		public void open(Configuration parameters) throws Exception {

			if (!this.isRegProc) {
				List <Object> labelRows = getRuntimeContext().getBroadcastVariable(LABEL_VALUES);
				Object[] labels = orderLabels(labelRows, positiveLableValueString);
				if (this.positiveLableValueString == null) {
					throw new RuntimeException("constrained logistic regression must set positive label!");
				}
				EvaluationUtil.ComparableLabel posLabel =
					new EvaluationUtil.ComparableLabel(this.positiveLableValueString, this.type);
				if (!posLabel.equals(new EvaluationUtil.ComparableLabel(labels[0].toString(), type))
					&& !posLabel.equals(new EvaluationUtil.ComparableLabel(labels[1].toString(), type))) {
					throw new RuntimeException("the user defined positive label is not in the data!");
				}
			}
		}

		@Override
		public Tuple3 <Double, Double, Vector> map(Row row) throws Exception {
			Double weight = weightIdx != -1 ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
			Double val = FeatureLabelUtil.getLabelValue(row, this.isRegProc,
				labelIdx, this.positiveLableValueString);
			if (featureIndices != null) {
				DenseVector vec = new DenseVector(featureIndices.length);
				for (int i = 0; i < featureIndices.length; ++i) {
					vec.set(i, ((Number) row.getField(featureIndices[i])).doubleValue());
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

	protected static Object[] orderLabels(Iterable <Object> unorderedLabelRows, String positiveLabel) {
		List <Object> tmpArr = new ArrayList <>();
		for (Object row : unorderedLabelRows) {
			tmpArr.add(row);
		}
		Object[] labels = tmpArr.toArray(new Object[0]);

		Preconditions.checkState((labels.length >= 2), "labels count should be more than 2 in classification algo.");
		String str1 = labels[1].toString();

		if (str1.equals(positiveLabel)) {
			Object t = labels[0];
			labels[0] = labels[1];
			labels[1] = t;
		}
		return labels;
	}

	protected static Object[] orderLabels(Object[] unorderedLabelRows, String positiveLabel) {

		Preconditions.checkState((unorderedLabelRows.length >= 2),
			"labels count should be more than 2 in classification algo.");
		String str1 = unorderedLabelRows[1].toString();

		if (str1.equals(positiveLabel)) {
			Object t = unorderedLabelRows[0];
			unorderedLabelRows[0] = unorderedLabelRows[1];
			unorderedLabelRows[1] = t;
		}
		return unorderedLabelRows;
	}

	/**
	 * @param constraints Constrains for linear model.
	 * @param params      Parameters.
	 * @param vectorSize  Vector size.
	 * @param modelType   Model type.
	 * @param session     Session.
	 * @param method      Method.
	 * @param countZero   Count Zero.
	 * @return OptimObjFunc, coefficientDim>
	 */
	private static Tuple2 <DataSet <OptimObjFunc>, DataSet <Integer>> getOptParam(DataSet <Row> constraints,
																				  Params params,
																				  DataSet <Integer> vectorSize,
																				  LinearModelType modelType,
																				  MLEnvironment session, String method,
																				  DataSet <DenseVector> countZero) {
		boolean hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
		String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
		String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
		if ("".equals(vectorColName)) {
			vectorColName = null;
		}
		if (org.apache.commons.lang3.ArrayUtils.isEmpty(featureColNames)) {
			featureColNames = null;
		}

		DataSet <Integer> coefficientDim;

		if (vectorColName != null && vectorColName.length() != 0) {
			coefficientDim = vectorSize;
		} else {
			assert featureColNames != null;
			coefficientDim = session.getExecutionEnvironment().fromElements(featureColNames.length
				+ (hasInterceptItem ? 1 : 0));
		}
		// Loss object function
		//checkout feasible constraint or not.
		DataSet <OptimObjFunc> objFunc = session.getExecutionEnvironment()
			.fromElements(getObjFunction(modelType, params))
			.map(new GetConstraint(featureColNames, hasInterceptItem, method,
				params.get(ScorecardTrainBatchOp.WITH_ELSE)))
			.withBroadcastSet(coefficientDim, "coef")
			.withBroadcastSet(constraints, "constraints")
			.withBroadcastSet(countZero, "countZero");
		return Tuple2.of(objFunc, coefficientDim);
	}

	/**
	 * optimize linear problem
	 *
	 * @param optParam  <OptimObjFunc, coefficientDim>
	 * @param trainData <weight, label, features>
	 * @return coefficient of linear problem. <coefVector, lossCurve>
	 */
	public static DataSet <Tuple2 <DenseVector, double[]>> optimize(Params params,
																	Tuple2 <DataSet <OptimObjFunc>, DataSet <Integer>>
																		optParam,
																	DataSet <Tuple3 <Double, Double, Vector>>
																		trainData,
																	String modelName, String method) {
		DataSet <OptimObjFunc> objFunc = optParam.f0;
		DataSet <Integer> coefficientDim = optParam.f1;

		if (params.contains(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD)) {
			switch (ConstrainedOptMethod.valueOf(method)) {
				case SQP:
					return new Sqp(objFunc, trainData, coefficientDim, params).optimize();
				case BARRIER:
					return new LogBarrier(objFunc, trainData, coefficientDim, params).optimize();
				case LBFGS:
					return new Lbfgs(objFunc, trainData, coefficientDim, params).optimize();
				case NEWTON:
					return new Newton(objFunc, trainData, coefficientDim, params).optimize();
				case ALM:
					return new Alm(objFunc, trainData, coefficientDim, params).optimize();
				default:
					throw new RuntimeException("do not support the " + method + " method!");
			}
		}
		//default opt method.
		return new Sqp(objFunc, trainData, coefficientDim, params).optimize();
	}

	protected enum ConstrainedOptMethod {
		SQP,
		BARRIER,
		LBFGS,
		NEWTON,
		ALM,
	}

	private static class GetConstraint extends RichMapFunction <OptimObjFunc,
		OptimObjFunc> {
		private static final long serialVersionUID = -7810872210451727729L;
		private int coefDim;
		private FeatureConstraint constraint;
		private final String[] featureColNames;
		private final boolean hasInterceptItem;
		private final ConstrainedOptMethod method;
		private DenseVector countZero = null;
		private final Map <String, Boolean> hasElse;

		GetConstraint(String[] featureColNames, boolean hasInterceptItem, String method,
					  Map <String, Boolean> withElse) {
			this.featureColNames = featureColNames;
			this.hasInterceptItem = hasInterceptItem;
			this.method = ConstrainedOptMethod.valueOf(method.toUpperCase());
			this.hasElse = withElse;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			coefDim = (int) getRuntimeContext().getBroadcastVariable("coef").get(0);
			constraint = (FeatureConstraint) ((Row) getRuntimeContext().getBroadcastVariable("constraints").get(0))
				.getField(0);
			if (constraint.fromScorecard()) {
				this.countZero = (DenseVector) getRuntimeContext().getBroadcastVariable("countZero").get(0);
			}
		}

		@Override
		public OptimObjFunc map(OptimObjFunc value) throws Exception {
			ConstraintObjFunc objFunc = (ConstraintObjFunc) value;
			if (!(ConstrainedOptMethod.LBFGS.equals(method) || ConstrainedOptMethod.NEWTON.equals(method))) {
				//将约束变成inequalityConstraint， inequalityItem， equalityConstraint， equalityItem四个矩阵放在objFunc里
				ConstrainedLocalOptimizer.extractConstraintsForFeatureAndBin(constraint, objFunc, featureColNames,
					coefDim, hasInterceptItem, countZero, hasElse);
				//checkout feasible constraint or not.
				int length = objFunc.equalityItem.size() + objFunc.inequalityItem.size();
				if (length != 0) {
					int dim = objFunc.equalityConstraint.numRows() != 0 ? objFunc.equalityConstraint.numCols() :
						objFunc.inequalityConstraint.numCols();
					double[] objData = new double[dim];
					LinearObjectiveFunction obj = new LinearObjectiveFunction(objData, 0);
					List <LinearConstraint> cons = new ArrayList <>();
					for (int i = 0; i < objFunc.equalityItem.size(); i++) {
						double[] constraint = new double[dim];
						System.arraycopy(objFunc.equalityConstraint.getRow(i), 0, constraint, 0, dim);
						double item = objFunc.equalityItem.get(i);
						cons.add(new LinearConstraint(constraint, Relationship.EQ, item));
					}
					for (int i = 0; i < objFunc.inequalityItem.size(); i++) {
						double[] constraint = new double[dim];
						System.arraycopy(objFunc.inequalityConstraint.getRow(i), 0, constraint, 0, dim);
						double item = objFunc.inequalityItem.get(i);
						cons.add(new LinearConstraint(constraint, Relationship.GEQ, item));
					}
					LinearConstraintSet conSet = new LinearConstraintSet(cons);
					try {
						PointValuePair pair = new SimplexSolver().optimize(obj, conSet, GoalType.MINIMIZE);
					} catch (Exception e) {
						throw new RuntimeException("infeasible constraint!", e);
					}
				}
			}

			return objFunc;
		}
	}

	/**
	 * Get obj function.
	 *
	 * @param modelType model type.
	 * @param params    parameters for train.
	 * @return Optimization object function.
	 */
	public static OptimObjFunc getObjFunction(LinearModelType modelType, Params params) {
		OptimObjFunc objFunc;
		// For different model type, we must set corresponding loss object function.
		if (modelType == LinearModelType.LinearReg) {
			objFunc = new ConstraintObjFunc(new SquareLossFunc(), params);
		} else if (modelType == LinearModelType.LR) {
			objFunc = new ConstraintObjFunc(new LogLossFunc(), params);
		} else if (modelType == LinearModelType.Divergence) {
			objFunc = new ConstraintObjFunc(new LogLossFunc(), params);
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
		return objFunc;
	}

	/**
	 * Write visualized data.
	 *
	 * @param modelRows  model data in row format.
	 * @param vectorSize vector Size.
	 */
	private void writeVizData(DataSet <Row> modelRows, DataSet <Integer> vectorSize) {
		VizDataWriterInterface writer = getVizDataWriter();
		if (writer == null) {
			return;
		}
		DataSet <Row> processedModelRows = modelRows.mapPartition(new RichMapPartitionFunction <Row, Row>() {
			private static final long serialVersionUID = -7146244281747193903L;

			@Override
			public void mapPartition(Iterable <Row> values, Collector <Row> out) {
				final int featureSize = (Integer) (getRuntimeContext()
					.getBroadcastVariable(VECTOR_SIZE).get(0));
				if (featureSize <= NUM_FEATURE_THRESHOLD) {
					values.forEach(out::collect);
				} else {
					String errorStr = "Not support models with #features > " + NUM_FEATURE_THRESHOLD;
					out.collect(Row.of(errorStr));
				}
			}
		}).withBroadcastSet(vectorSize, VECTOR_SIZE).setParallelism(1);
		VizDataWriterForModelInfo.writeModelInfo(writer, this.getClass().getSimpleName(),
			this.getOutputTable().getSchema(), processedModelRows, getParams());
	}

	/**
	 * Create meta info.
	 */
	public static class CreateMeta implements MapPartitionFunction <Object, Params> {
		private static final long serialVersionUID = -7148219424266582224L;
		private final String modelName;
		private final LinearModelType modelType;
		private final boolean hasInterceptItem;
		private final String vectorColName;
		private final String labelName;
		private final boolean calcLabel;
		private final String positiveLabel;

		public CreateMeta(String modelName, LinearModelType modelType,
						  Params params, boolean calcLabel, String positiveLabel) {
			this.modelName = modelName;
			this.modelType = modelType;
			this.hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
			this.vectorColName = params.get(LinearTrainParams.VECTOR_COL);
			this.labelName = params.get(LinearTrainParams.LABEL_COL);
			this.calcLabel = calcLabel;
			this.positiveLabel = positiveLabel;
		}

		@Override
		public void mapPartition(Iterable <Object> rows, Collector <Params> metas) throws Exception {
			Object[] labels = null;
			if (calcLabel) {
				labels = orderLabels(rows, positiveLabel);
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
	 * The size of coefficient. Transform dimension of trainData, if has Intercept item, dimension++.
	 */
	private static class DimTrans extends AbstractRichFunction
		implements MapFunction <Integer, Integer> {
		private static final long serialVersionUID = 1997987979691400583L;
		private final boolean hasInterceptItem;
		private Integer featureDim = null;

		public DimTrans(boolean hasInterceptItem) {
			this.hasInterceptItem = hasInterceptItem;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.featureDim = (Integer) getRuntimeContext()
				.getBroadcastVariable(VECTOR_SIZE).get(0);
		}

		@Override
		public Integer map(Integer integer) throws Exception {
			return this.featureDim + (this.hasInterceptItem ? 1 : 0);
		}
	}

	protected static Tuple3 <DataSet <Integer>, DataSet <DenseVector[]>, DataSet <DenseVector>> getStatInfo(
		DataSet <Tuple3 <Double, Double, Vector>> trainData,
		final boolean standardization) {
		//may pass the param isScorecard, so that only need to get it when scorecard.
		DataSet <BaseVectorSummary> summary = StatisticsHelper.summary(trainData.map(
			new MapFunction <Tuple3 <Double, Double, Vector>, Vector>() {
				private static final long serialVersionUID = 6207307350053531656L;

				@Override
				public Vector map(Tuple3 <Double, Double, Vector> value) {
					return value.f2;
				}
			}).withForwardedFields());
		DataSet <DenseVector> countZero = summary.map(new MapFunction <BaseVectorSummary, DenseVector>() {

			private static final long serialVersionUID = 2322849507320367330L;

			@Override
			public DenseVector map(BaseVectorSummary value) {
				if (value instanceof SparseVectorSummary) {
					return (DenseVector) ((SparseVectorSummary) value).numNonZero();
				}
				return new DenseVector(0);
			}
		});
		if (standardization) {
			DataSet <Integer> coefficientDim = summary.map(new MapFunction <BaseVectorSummary, Integer>() {
				private static final long serialVersionUID = -8051245706564042978L;

				@Override
				public Integer map(BaseVectorSummary value) {
					return value.vectorSize();
				}
			});
			DataSet <DenseVector[]> meanVar = summary.map(new MapFunction <BaseVectorSummary, DenseVector[]>() {
				private static final long serialVersionUID = -6992060467629008691L;

				@Override
				public DenseVector[] map(BaseVectorSummary value) {
					if (value instanceof SparseVectorSummary) {
						// If train data format is sparse vector, use maxAbs as variance and set mean zero,
						// then, the standardization operation will turn into a scale operation.
						// Because if do standardization to sparse vector, vector will be convert to be a dense one.
						DenseVector max = ((SparseVector) value.max()).toDenseVector();
						DenseVector min = ((SparseVector) value.min()).toDenseVector();
						for (int i = 0; i < max.size(); ++i) {
							max.set(i, Math.max(Math.abs(max.get(i)), Math.abs(min.get(i))));
							min.set(i, 0.0);
						}
						return new DenseVector[] {min, max};
					} else {
						return new DenseVector[] {(DenseVector) value.mean(),
							(DenseVector) value.standardDeviation()};
					}
				}
			});
			return Tuple3.of(coefficientDim, meanVar, countZero);
		} else {
			// If not do standardization, the we use mapReduce to get vector Dim. Mean and var set zero vector.
			DataSet <Integer> coefficientDim = trainData.mapPartition(
				new MapPartitionFunction <Tuple3 <Double, Double, Vector>, Integer>() {
					private static final long serialVersionUID = 3426157421982727224L;

					@Override
					public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> values, Collector <Integer>
						out) {
						int ret = -1;
						for (Tuple3 <Double, Double, Vector> val : values) {
							if (val.f2 instanceof DenseVector) {
								ret = ((DenseVector) val.f2).getData().length;
								break;
							} else {

								int[] ids = ((SparseVector) val.f2).getIndices();
								for (int id : ids) {
									ret = Math.max(ret, id + 1);
								}
							}
							ret = Math.max(ret, val.f2.size());
						}

						out.collect(ret);
					}
				}).reduceGroup(new GroupReduceFunction <Integer, Integer>() {
				private static final long serialVersionUID = 2752381384411882555L;

				@Override
				public void reduce(Iterable <Integer> values, Collector <Integer> out) {
					int ret = -1;
					for (int vSize : values) {
						ret = Math.max(ret, vSize);
					}
					out.collect(ret);
				}
			});

			DataSet <DenseVector[]> meanVar = coefficientDim.map(new MapFunction <Integer, DenseVector[]>() {
				private static final long serialVersionUID = 5448632685946933829L;

				@Override
				public DenseVector[] map(Integer value) {
					return new DenseVector[] {new DenseVector(0), new DenseVector(0)};
				}
			});
			return Tuple3.of(coefficientDim, meanVar, countZero);
		}
	}

	/**
	 * Do standardization and interception to train data.
	 *
	 * @param initData initial data.
	 * @param params   train parameters.
	 * @param meanVar  mean and variance of train data.
	 * @return train data after standardization.
	 */
	protected static DataSet <Tuple3 <Double, Double, Vector>> preProcess(
		DataSet <Tuple3 <Double, Double, Vector>> initData,
		Params params,
		DataSet <DenseVector[]> meanVar) {
		// Get parameters.
		final boolean hasInterceptItem = params.get(LinearTrainParams.WITH_INTERCEPT);
		final boolean standardization = params.get(LinearTrainParams.STANDARDIZATION);

		return initData.map(
			new RichMapFunction <Tuple3 <Double, Double, Vector>, Tuple3 <Double, Double, Vector>>() {
				private static final long serialVersionUID = -5342628140781184056L;
				private DenseVector[] meanVar;

				@Override
				public void open(Configuration parameters) {
					this.meanVar = (DenseVector[]) getRuntimeContext()
						.getBroadcastVariable(MEAN_VAR).get(0);
					modifyMeanVar(standardization, meanVar);
				}

				@Override
				public Tuple3 <Double, Double, Vector> map(Tuple3 <Double, Double, Vector> value) {

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
								bVector = (DenseVector) aVector;
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
								bVector = (DenseVector) aVector;
							}
						}
						return Tuple3.of(value.f0, value.f1, bVector);

					} else {
						SparseVector bVector = (SparseVector) aVector;

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
	 * Get label info: including label values and label type.
	 *
	 * @param in        input train data in BatchOperator format.
	 * @param params    train parameters.
	 * @param isRegProc not use label, include linear regression which not in scorecard.
	 * @return label info.
	 */
	protected static Tuple2 <DataSet <Object>, TypeInformation> getLabelInfo(BatchOperator in,
																			 Params params,
																			 boolean isRegProc) {
		String labelName = params.get(LinearTrainParams.LABEL_COL);
		// Prepare label values
		DataSet <Object> labelValues;
		TypeInformation <?> labelType;
		if (isRegProc) {
			labelType = Types.DOUBLE;
			labelValues = MLEnvironmentFactory.get(in.getMLEnvironmentId())
				.getExecutionEnvironment().fromElements(new Object());
		} else {
			labelType = in.getColTypes()[TableUtil.findColIndexWithAssertAndHint(in.getColNames(), labelName)];
			labelValues = Preprocessing.distinctLabels(Preprocessing.select(in, new String[] {labelName})
				.getDataSet().map(new MapFunction <Row, Object>() {
					private static final long serialVersionUID = -419245917074561046L;

					@Override
					public Object map(Row value) {
						return value.getField(0);
					}
				})).flatMap(new FlatMapFunction <Object[], Object>() {
				private static final long serialVersionUID = -5089566319196319692L;

				@Override
				public void flatMap(Object[] value, Collector <Object> out) {
					for (Object obj : value) {
						out.collect(obj);
					}
				}
			});
		}
		return Tuple2.of(labelValues, labelType);
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

	private static class BuildLabels implements
		FlatMapFunction <Tuple3 <DenseVector[], Object[], Integer[]>, Object[]> {
		private final boolean isRegProc;
		private final String positiveLabel;

		BuildLabels(boolean isRegProc, String positiveLabel) {
			this.isRegProc = isRegProc;
			this.positiveLabel = positiveLabel;
		}

		private static final long serialVersionUID = 5375954526931728363L;

		@Override
		public void flatMap(Tuple3 <DenseVector[], Object[], Integer[]> value,
							Collector <Object[]> out)
			throws Exception {
			if (!isRegProc) {
				Preconditions.checkState((value.f1.length == 2),
					"labels count should be 2 in in classification algo.");

				out.collect(orderLabels(value.f1, positiveLabel));
			} else {
				out.collect(value.f1);
			}
		}
	}
}