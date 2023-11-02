package com.alibaba.alink.operator.batch.finance;

import  org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BinningPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.BinningTrainBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.finance.ScorecardModelInfo;
import com.alibaba.alink.operator.common.finance.ScorecardModelInfoBatchOp;
import com.alibaba.alink.operator.common.finance.VizData;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.BaseStepWiseSelectorBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.ClassificationSelectorResult;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorResult;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.StepWiseType;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.ModelSummaryHelper;
import com.alibaba.alink.operator.common.optim.ConstraintBetweenBins;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.finance.BaseStepwiseSelectorParams;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.FitScaleParams;
import com.alibaba.alink.params.finance.HasConstrainedOptimizationMethod;
import com.alibaba.alink.params.finance.HasPdo;
import com.alibaba.alink.params.finance.HasScaledValue;
import com.alibaba.alink.params.finance.ScorecardTrainParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueStringDefaultAs1;
import com.alibaba.alink.params.shared.linear.HasStandardization;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.feature.BinningModel;
import com.alibaba.alink.pipeline.finance.ScoreModel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA),
})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL)
})

@ParamSelectColumnSpec(name = "selectCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "forceSelectedCols")
@FeatureColsVectorColMutexRule
@NameCn("评分卡训练")
@NameEn("Score Trainer")
public class ScorecardTrainBatchOp extends BatchOperator <ScorecardTrainBatchOp>
	implements ScorecardTrainParams <ScorecardTrainBatchOp>, AlinkViz <ScorecardTrainBatchOp>,
	WithModelInfoBatchOp <ScorecardModelInfo, ScorecardTrainBatchOp, ScorecardModelInfoBatchOp> {
	private static final long serialVersionUID = 9216204663345544968L;
	static double SCALE_A = 1.0;
	static double SCALE_B = 0.0;

	public static String BINNING_OUTPUT_COL = "BINNING_PREDICT";

	private static String UNSCALED_MODEL = "unscaledModel";
	private static String SCALED_MODEL = "scaledModel";
	private static String BIN_COUNT = "binCount";
	private static String INTERCEPT = "intercept";
	private static String STEPWISE_MODEL = "stepwiseModel";
	private static String SCORECARD_MODEL = "scorecardModel";

	public static ParamInfo <Map> WITH_ELSE = ParamInfoFactory
		.createParamInfo("withElse", Map.class)
		.setDescription("has else or not")
		.setHasDefaultValue(null)
		.build();

	public static ParamInfo <Boolean> IN_SCORECARD = ParamInfoFactory
		.createParamInfo("inScorecard", Boolean.class)
		.setDescription("calculate linear model in scorecard else or not")
		.setHasDefaultValue(false)
		.build();

	public ScorecardTrainBatchOp(Params params) {
		super(params);
	}

	public ScorecardTrainBatchOp() {
		super(null);
	}

	public static Tuple2 <Double, Double> loadScaleInfo(Params params) {
		if (!params.contains(FitScaleParams.SCALED_VALUE) && !params.contains(FitScaleParams.ODDS) && !params.contains(
			FitScaleParams.PDO)) {
			return Tuple2.of(SCALE_A, SCALE_B);
		}
		if (params.contains(FitScaleParams.SCALED_VALUE) && params.contains(FitScaleParams.ODDS) && params.contains(
			FitScaleParams.PDO)) {
			double odds = params.get(FitScaleParams.ODDS);
			double logOdds = Math.log(odds);
			double scaleA = (Math.log(odds * 2) - logOdds) / params.get(HasPdo.PDO);
			double scaleB = logOdds - scaleA * params.get(HasScaledValue.SCALED_VALUE);
			return Tuple2.of(scaleA, scaleB);
		}
		return Tuple2.of(SCALE_A, SCALE_B);
	}

	@Override
	public ScorecardTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> data = inputs[0];
		BatchOperator <?> binningModel = null;
		if (inputs.length > 1) {
			binningModel = inputs[1];
		}
		BatchOperator <?> constraint = null;
		if (inputs.length == 3) {
			constraint = inputs[2];
		}
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(data.getSchema(), this.getLabelCol());
		List <TransformerBase <?>> finalModel = new ArrayList <>();

		Boolean withSelector = getWithSelector();

		//binning
		DataSet <FeatureBinsCalculator> featureBorderDataSet = null;
		String[] selectedCols = this.getSelectedCols();
		String[] outputCols = this.getSelectedCols();
		String outputCol = null;
		Encode encode = binningModel == null ? Encode.NULL : getEncode();
		switch (encode) {
			case ASSEMBLED_VECTOR:
			case WOE: {
				Preconditions.checkNotNull(binningModel, "BinningModel is empty!");
				featureBorderDataSet = FeatureBinsUtil.parseFeatureBinsModel(binningModel.getDataSet());
				Params binningPredictParams = getBinningOutputParams(getParams(), withSelector);
				data = new BinningPredictBatchOp(binningPredictParams).linkFrom(binningModel, data);
				finalModel.add(setBinningModelData(binningModel, binningPredictParams));
				outputCols = getParams().get(HasOutputColsDefaultAsNull.OUTPUT_COLS);
				outputCol = getParams().get(HasOutputCol.OUTPUT_COL);
				break;
			}
			case NULL: {
				TableUtil.assertNumericalCols(data.getSchema(), selectedCols);
				break;
			}
			default: {
				throw new RuntimeException("Not support " + encode.name());
			}
		}
		Map <String, Boolean> withElse = withElse(inputs[0].getSchema(), selectedCols);
		constraint = unionConstraint(constraint, featureBorderDataSet, encode, selectedCols, withElse);

		//model
		BatchOperator <?> model;
		DataSet <SelectorResult> modeSummary = null;

		if (!withSelector) {
			Params linearTrainParams = new Params()
				.set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, getParams().get(HasMLEnvironmentId.ML_ENVIRONMENT_ID))
				.set(HasWithIntercept.WITH_INTERCEPT, true)
				.set(HasFeatureColsDefaultAsNull.FEATURE_COLS, outputCols)
				.set(HasVectorColDefaultAsNull.VECTOR_COL, outputCol)
				.set(HasLabelCol.LABEL_COL, getLabelCol())
				.set(HasWeightColDefaultAsNull.WEIGHT_COL, getWeightCol())
				.set(HasPositiveLabelValueStringDefaultAs1.POS_LABEL_VAL_STR, getPositiveLabelValueString())
				.set(WITH_ELSE, withElse)
				.set(IN_SCORECARD, true)
				.set(HasEpsilonDefaultAs0000001.EPSILON, getEpsilon())
				.set(HasL2.L_2, getL2())
				.set(HasL1.L_1, getL1())
				.set(HasStandardization.STANDARDIZATION, true)
				.set(HasMaxIterDefaultAs100.MAX_ITER, getMaxIter())
				.set(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD, getConstOptimMethod());

			if (getConstOptimMethod() == null) {
				linearTrainParams.set(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD,
					null == constraint ? ConstOptimMethod.LBFGS : ConstOptimMethod.SQP);
			}

			ConstOptimMethod optMethod = linearTrainParams.get(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD);
			boolean useConstraint = (encode == Encode.ASSEMBLED_VECTOR) &&
				(optMethod == ConstOptimMethod.SQP || optMethod == ConstOptimMethod.Barrier);
			if (ModelSummaryHelper.isLinearRegression(getLinearModelType().toString())) {
				if (useConstraint) {
					model = new ConstrainedLinearRegTrainBatchOp(linearTrainParams).linkFrom(data, constraint);
				} else {
					model = new ConstrainedLinearRegTrainBatchOp(linearTrainParams).linkFrom(data, null);
				}
			} else if (ModelSummaryHelper.isLogisticRegression(getLinearModelType().toString())) {
				if (useConstraint) {
					model = new ConstrainedLogisticRegressionTrainBatchOp(linearTrainParams).linkFrom(data,
						constraint);
				} else {
					model = new ConstrainedLogisticRegressionTrainBatchOp(linearTrainParams).linkFrom(data, null);
				}
			} else {
				model = new ConstrainedDivergenceTrainBatchOp(linearTrainParams).linkFrom(data,
					constraint);
			}
		} else {
			Params stepwiseParams = new Params()
				.set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, getParams().get(HasMLEnvironmentId.ML_ENVIRONMENT_ID))
				.set(BaseStepwiseSelectorParams.SELECTED_COLS, outputCols)
				.set(BaseStepwiseSelectorParams.LABEL_COL, getLabelCol())
				.set(BaseStepwiseSelectorParams.LINEAR_MODEL_TYPE, getLinearModelType())
				.set(BaseStepwiseSelectorParams.OPTIM_METHOD, getConstOptimMethod().name())
				.set(BaseStepwiseSelectorParams.ALPHA_ENTRY, getAlphaEntry())
				.set(BaseStepwiseSelectorParams.ALPHA_STAY, getAlphaStay())
				.set(HasPositiveLabelValueStringDefaultAs1.POS_LABEL_VAL_STR, getPositiveLabelValueString())
				.set(IN_SCORECARD, true)
				.set(BaseStepwiseSelectorParams.WITH_VIZ, false);

			if (ScorecardTrainParams.Encode.ASSEMBLED_VECTOR == encode) {
				stepwiseParams.set(BaseStepwiseSelectorParams.STEP_WISE_TYPE, StepWiseType.marginalContribution);
			} else {
				if (LinearModelType.LR == getLinearModelType()) {
					stepwiseParams.set(BaseStepwiseSelectorParams.STEP_WISE_TYPE, StepWiseType.scoreTest);
				} else {
					stepwiseParams.set(BaseStepwiseSelectorParams.STEP_WISE_TYPE, StepWiseType.fTest);
				}
			}
			if (getParams().contains(ScorecardTrainParams.FORCE_SELECTED_COLS)) {
				String[] forceCols = getForceSelectedCols();
				if (forceCols != null && forceCols.length != 0) {
					int[] forceColsIndices = TableUtil.findColIndicesWithAssertAndHint(getSelectedCols(), forceCols);
					stepwiseParams.set(BaseStepwiseSelectorParams.FORCE_SELECTED_COLS, forceColsIndices);
				}
			}

			BaseStepWiseSelectorBatchOp stepwiseModel = new BaseStepWiseSelectorBatchOp(stepwiseParams).linkFrom(data,
				constraint);
			model = stepwiseModel.getSideOutput(0);
			modeSummary = stepwiseModel.getStepWiseSummary();
		}

		Preconditions.checkArgument(model != null, "Unscaled model is not set!");

		finalModel.add(
			setScoreModelData(
				model,
				new Params().set(
					HasMLEnvironmentId.ML_ENVIRONMENT_ID,
					getParams().get(HasMLEnvironmentId.ML_ENVIRONMENT_ID)
				),
				labelType
			)
		);

		DataSet <LinearModelData> linearModelDataDataSet = model
			.getDataSet()
			.mapPartition(new LoadLinearModel())
			.setParallelism(1);

		VizDataWriterInterface writer = this.getVizDataWriter();

		//statistic viz
		statViz(writer, modeSummary, encode, withSelector,
			data, linearModelDataDataSet, outputCol, outputCols);

		//fit scale
		DataSet <LinearModelData> scaledLinearModelDataSet = linearModelDataDataSet;
		if (getScaleInfo()) {
			Preconditions.checkArgument(
				null != getOdds() && null != getPdo() && null != getScaledValue(),
				"ScaledValue/Pdo/Odds must be set!"
			);
			scaledLinearModelDataSet = linearModelDataDataSet.map(new ScaleLinearModel(getParams()));
			finalModel.add(setScoreModelData(
				BatchOperator.fromTable(DataSetConversionUtil
					.toTable(getParams().get(HasMLEnvironmentId.ML_ENVIRONMENT_ID),
						scaledLinearModelDataSet.flatMap(new SerializeLinearModel()),
						new LinearModelDataConverter(labelType).getModelSchema())

				),
				new Params()
					.set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, getParams().get(HasMLEnvironmentId.ML_ENVIRONMENT_ID)),
				labelType));
		}

		//pipeline model transform
		BatchOperator <?> savedModel = new PipelineModel(finalModel.toArray(new TransformerBase[0])).save();

		DataSet <Row> modelRows = savedModel
			.getDataSet()
			.map(new PipelineModelMapper.ExtendPipelineModelRow(selectedCols.length + 1));

		TypeInformation[] selectedTypes = new TypeInformation[this.getSelectedCols().length];
		Arrays.fill(selectedTypes, labelType);

		TableSchema modelSchema = PipelineModelMapper.getExtendModelSchema(
			savedModel.getSchema(), this.getSelectedCols(), selectedTypes);

		this.setOutput(modelRows, modelSchema);

		if (null != featureBorderDataSet) {
			featureBorderDataSet = updateStatisticsInFeatureBins(getParams(), inputs[0], binningModel,
				featureBorderDataSet);

			DataSet <VizData.ScorecardVizData> vizData;
			if (withSelector) {
				vizData = featureBorderDataSet
					.flatMap(new FeatureBinsToScorecard(getParams(), withSelector))
					.withBroadcastSet(linearModelDataDataSet, UNSCALED_MODEL)
					.withBroadcastSet(scaledLinearModelDataSet, SCALED_MODEL)
					.withBroadcastSet(featureNameBinCount(featureBorderDataSet), BIN_COUNT)
					.withBroadcastSet(modeSummary, STEPWISE_MODEL)
				;
			} else {
				vizData = featureBorderDataSet
					.flatMap(new FeatureBinsToScorecard(getParams(), withSelector))
					.withBroadcastSet(linearModelDataDataSet, UNSCALED_MODEL)
					.withBroadcastSet(scaledLinearModelDataSet, SCALED_MODEL)
					.withBroadcastSet(featureNameBinCount(featureBorderDataSet), BIN_COUNT);
			}

			if (writer != null) {
				DataSet <Row> dummy = vizData
					.mapPartition(new VizDataWriter(writer))
					.setParallelism(1)
					.name("WriteVizData");
				DataSetUtil.linkDummySink(dummy);
			}
		}

		if (writer != null) {
			DataSet <Row> dummy = modelRows
				.mapPartition(new WriteModelPMML(writer, modelSchema))
				.setParallelism(1);
			DataSetUtil.linkDummySink(dummy);
		}
		return this;
	}

	private void statViz(VizDataWriterInterface writer,
						 DataSet <SelectorResult> stepwiseSummary, Encode encode, Boolean withSelector,
						 BatchOperator data, DataSet <LinearModelData> linearModelDataDataSet, String outputCol,
						 String[] outputCols) {
		if ((encode == Encode.WOE || encode == Encode.NULL) || withSelector) {
			DataSet <SelectorResult> selectorResult;
			if (withSelector) {
				selectorResult = stepwiseSummary;
			} else {
				selectorResult = ModelSummaryHelper.calModelSummary(data, getLinearModelType(),
					linearModelDataDataSet, outputCol, outputCols, getLabelCol());
			}
			DataSetUtil.linkDummySink(selectorResult.flatMap(new StepSummaryVizDataWriter(writer)));
		}
	}

	private static class WriteModelPMML implements MapPartitionFunction <Row, Row> {
		private VizDataWriterInterface writer;

		private final String[] modelFieldNames;

		/**
		 * Field types of the model.
		 */
		private final DataType[] modelFieldTypes;

		public WriteModelPMML(VizDataWriterInterface writer, TableSchema schema) {
			this.writer = writer;
			this.modelFieldNames = schema.getFieldNames();
			this.modelFieldTypes = schema.getFieldDataTypes();
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <Row> collector) {
			List <Row> modelRows = new ArrayList <>();
			iterable.forEach(modelRows::add);
			ScorecardModelInfo summary = new ScorecardModelInfo(
				modelRows, TableSchema.builder().fields(modelFieldNames, modelFieldTypes).build()
			);
			//System.out.println(summary.getPMML());
			writer.writeBatchData(3L, summary.getPMML(), System.currentTimeMillis());
		}
	}

	private static DataSet <FeatureBinsCalculator> updateStatisticsInFeatureBins(Params params,
																				 BatchOperator data,
																				 BatchOperator binningModel,
																				 DataSet <FeatureBinsCalculator>
																					 featureBorderDataSet) {
		BinningPredictBatchOp op = new BinningPredictBatchOp(BinningTrainBatchOp.encodeIndexForWoeTrainParams(
			params.get(HasSelectedCols.SELECTED_COLS), params.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID)));

		op.linkFrom(binningModel, data);

		return BinningTrainBatchOp.setFeatureBinsTotalAndWoe(featureBorderDataSet, op, params);
	}

	private static class VizDataWriter extends RichMapPartitionFunction <VizData.ScorecardVizData, Row> {
		private static final long serialVersionUID = -6344386801175328688L;
		private VizDataWriterInterface writer;

		public VizDataWriter(VizDataWriterInterface writer) {
			this.writer = writer;
		}

		@Override
		public void mapPartition(Iterable <VizData.ScorecardVizData> iterable, Collector <Row> collector)
			throws Exception {

			Map <String, TreeSet <VizData.ScorecardVizData>> map = new HashMap <>();
			for (VizData.ScorecardVizData data : iterable) {
				TreeSet <VizData.ScorecardVizData> list = map.computeIfAbsent(data.featureName,
					k -> new TreeSet <>(VizData.VizDataComparator));
				if (!data.featureName.equals(INTERCEPT) || list.size() < 1) {
					list.add(data);
				}
			}
			//System.out.println(JsonConverter.toJson(map));
			writer.writeBatchData(0L, JsonConverter.toJson(map), System.currentTimeMillis());
		}
	}

	private static Map <String, Boolean> withElse(TableSchema tableSchema, String[] selectedCols) {
		Map <String, Boolean> withElse = new HashMap <>();
		for (String s : selectedCols) {
			withElse.put(s, !TableUtil.isSupportedNumericType(TableUtil.findColTypeWithAssertAndHint(tableSchema, s)));
		}
		return withElse;
	}

	private BatchOperator unionConstraint(BatchOperator constraint,
										  DataSet <FeatureBinsCalculator> featureBorderDataSet,
										  Encode encode,
										  String[] selectedCols,
										  Map <String, Boolean> withElse) {
		if (encode.equals(Encode.ASSEMBLED_VECTOR)) {
			Preconditions.checkNotNull(featureBorderDataSet, "Binning Model is empty!");
			BatchOperator binCountConstraint = binningModelToConstraint(featureBorderDataSet, selectedCols,
				this.getMLEnvironmentId());
			if (constraint == null) {
				constraint = binCountConstraint;
			} else {
				DataSet <Row> binConstraintDataSet = binCountConstraint.getDataSet();
				DataSet <Row> udConstraintDataSet = constraint.getDataSet();
				//withElse will not be null.
				DataSet <Row> cons = binConstraintDataSet.map(new MapConstraints(withElse))
					.withBroadcastSet(udConstraintDataSet, "udConstraint");
				constraint = new DataSetWrapperBatchOp(cons,
					constraint.getColNames(),
					new TypeInformation[] {TypeInformation.of(FeatureConstraint.class)})
					.setMLEnvironmentId(this.getMLEnvironmentId());
			}
		} else {
			constraint = null;
		}
		return constraint;
	}

	private static class StepSummaryVizDataWriter implements FlatMapFunction <SelectorResult, Row> {
		private static final long serialVersionUID = -2495790734379602318L;
		private VizDataWriterInterface writer;

		public StepSummaryVizDataWriter(VizDataWriterInterface writer) {
			this.writer = writer;
		}

		@Override
		public void flatMap(SelectorResult val, Collector <Row> collector) throws Exception {
			String type = "linearReg";
			if (val instanceof ClassificationSelectorResult) {
				type = "classification";
			}
			val.selectedCols = trimCols(val.selectedCols, BINNING_OUTPUT_COL);
			writer.writeBatchData(1L, type, System.currentTimeMillis());
			writer.writeBatchData(2L, val.toVizData(), System.currentTimeMillis());
		}
	}

	public static String[] trimCols(String[] cols, String trimStr) {
		if (cols == null) {
			return null;
		}
		String[] trimCols = new String[cols.length];
		for (int i = 0; i < trimCols.length; i++) {
			int index = cols[i].lastIndexOf(trimStr);
			if (index < 0) {
				trimCols[i] = cols[i];
			} else {
				trimCols[i] = cols[i].substring(0, index);
			}
		}
		return trimCols;
	}

	public static class FeatureBinsToScorecard
		extends RichFlatMapFunction <FeatureBinsCalculator, VizData.ScorecardVizData> {
		private static final long serialVersionUID = 332595127422145833L;
		private ScorecardTransformData transformData;

		public FeatureBinsToScorecard(Params params, boolean withSelector) {
			transformData = new ScorecardTransformData(params.get(ScorecardTrainParams.SCALE_INFO),
				params.get(ScorecardTrainParams.SELECTED_COLS),
				Encode.WOE.equals(params.get(ScorecardTrainParams.ENCODE)),
				params.get(ScorecardTrainParams.DEFAULT_WOE),
				withSelector);
		}

		@Override
		public void open(Configuration configuration) {
			transformData.unscaledModel = getModelData((LinearModelData)
				this.getRuntimeContext().getBroadcastVariable(UNSCALED_MODEL).get(0), transformData);
			if (transformData.isScaled) {
				transformData.scaledModel = getModelData((LinearModelData)
					this.getRuntimeContext().getBroadcastVariable(SCALED_MODEL).get(0), transformData);
			}
			if (!transformData.isWoe) {
				List <Tuple2 <String, Integer>> binCounts = this.getRuntimeContext().getBroadcastVariable(BIN_COUNT);
				Map <String, Integer> nameBinCountMap = new HashMap <>();
				binCounts.forEach(binCount -> nameBinCountMap.put(binCount.f0, binCount.f1));
				initializeStartIndex(transformData, nameBinCountMap);
			}
			if (transformData.withSelector) {
				SelectorResult selectorSummary = (SelectorResult) this.getRuntimeContext().getBroadcastVariable(
					STEPWISE_MODEL).get(0);
				transformData.stepwiseSelectedCols = trimCols(
					BaseStepWiseSelectorBatchOp
						.getSCurSelectedCols(transformData.selectedCols, selectorSummary.selectedIndices),
					BINNING_OUTPUT_COL);
			}
		}

		@Override
		public void flatMap(FeatureBinsCalculator featureBinsCalculator, Collector <VizData.ScorecardVizData> out) {
			featureBinsCalculator.calcStatistics();
			String featureColName = featureBinsCalculator.getFeatureName();
			transformData.featureIndex = TableUtil.findColIndex(transformData.selectedCols,
				featureBinsCalculator.getFeatureName());
			if (transformData.featureIndex < 0) {
				return;
			}
			featureBinsCalculator.splitsArrayToInterval();
			if (null != featureBinsCalculator.bin.nullBin) {
				VizData.ScorecardVizData vizData = transform(featureBinsCalculator, featureBinsCalculator.bin.nullBin,
					FeatureBinsUtil.NULL_LABEL, transformData);
				vizData.index = -1L;
				out.collect(vizData);
			}
			if (null != featureBinsCalculator.bin.elseBin) {
				VizData.ScorecardVizData vizData = transform(featureBinsCalculator, featureBinsCalculator.bin.elseBin,
					FeatureBinsUtil.ELSE_LABEL, transformData);
				vizData.index = -2L;
				out.collect(vizData);
			}
			if (null != featureBinsCalculator.bin.normBins) {
				for (Bins.BaseBin bin : featureBinsCalculator.bin.normBins) {
					out.collect(
						transform(featureBinsCalculator, bin, bin.getValueStr(featureBinsCalculator.getColType()),
							transformData));
				}
			}
			//write efficient
			VizData.ScorecardVizData firstLine = new VizData.ScorecardVizData(featureBinsCalculator.getFeatureName(),
				null,
				null);
			firstLine.total = featureBinsCalculator.getTotal();
			firstLine.positive = featureBinsCalculator.getPositiveTotal();
			if (firstLine.total != null && firstLine.positive != null) {
				firstLine.negative = firstLine.total - firstLine.positive;
				firstLine.positiveRate = 100.0;
				firstLine.negativeRate = 100.0;
			}
			if (transformData.isWoe) {
				int linearCoefIndex = findLinearModelCoefIdx(featureColName, null, transformData);
				firstLine.unscaledValue = FeatureBinsUtil.keepGivenDecimal(
					getLinearCoef(transformData.unscaledModel, linearCoefIndex),
					3);
				firstLine.scaledValue = transformData.isScaled ? FeatureBinsUtil.keepGivenDecimal(
					getLinearCoef(transformData.scaledModel, linearCoefIndex), 3) : null;
				out.collect(firstLine);
			} else {
				out.collect(new VizData.ScorecardVizData(featureBinsCalculator.getFeatureName(), null, null));
			}
			//write intercept
			VizData.ScorecardVizData intercept = new VizData.ScorecardVizData(INTERCEPT, null, null);
			intercept.unscaledValue = FeatureBinsUtil.keepGivenDecimal(transformData.unscaledModel[0], 3);
			intercept.scaledValue = transformData.isScaled ? FeatureBinsUtil.keepGivenDecimal(
				transformData.scaledModel[0], 3) : null;
			out.collect(intercept);

		}

		public static VizData.ScorecardVizData transform(FeatureBinsCalculator featureBinsCalculator,
														 Bins.BaseBin bin,
														 String label,
														 ScorecardTransformData transformData) {
			//write efficients, must be calculate first
			VizData.ScorecardVizData vizData = new VizData.ScorecardVizData(featureBinsCalculator.getFeatureName(),
				bin.getIndex(),
				label);
			int binIndex = bin.getIndex().intValue();
			String featureColName = featureBinsCalculator.getFeatureName();

			int linearModelCoefIdx = findLinearModelCoefIdx(featureColName, binIndex, transformData);

			if (linearModelCoefIdx >= 0) {
				vizData.unscaledValue = FeatureBinsUtil.keepGivenDecimal(
					getModelValue(linearModelCoefIdx, bin.getWoe(), transformData.unscaledModel, transformData.isWoe,
						transformData.defaultWoe), 3);
				vizData.scaledValue = transformData.isScaled ? FeatureBinsUtil.keepGivenDecimal(
					getModelValue(linearModelCoefIdx, bin.getWoe(), transformData.scaledModel, transformData.isWoe,
						transformData.defaultWoe), 0) : null;
			}

			//change the statistics of the bin, must be set after the efficients are set
			featureBinsCalculator.calcBinStatistics(bin);
			vizData.total = bin.getTotal();
			vizData.positive = bin.getPositive();
			vizData.negative = bin.getNegative();
			vizData.positiveRate = bin.getPositiveRate();
			vizData.negativeRate = bin.getNegativeRate();
			vizData.woe = bin.getWoe();

			vizData.positiveRate = (null == vizData.positiveRate ? null : FeatureBinsUtil.keepGivenDecimal(
				vizData.positiveRate * 100, 2));
			vizData.negativeRate = (null == vizData.negativeRate ? null : FeatureBinsUtil.keepGivenDecimal(
				vizData.negativeRate * 100, 2));

			return vizData;
		}

		public static void initializeStartIndex(ScorecardTransformData transformData,
												Map <String, Integer> nameBinCountMap) {
			transformData.startIndex = new int[transformData.selectedCols.length];
			int i = 1;
			for (; i < transformData.selectedCols.length; i++) {
				transformData.startIndex[i] = nameBinCountMap.get(
					transformData.selectedCols[i - 1]) + transformData.startIndex[i - 1];
			}
			if (!transformData.withSelector) {
				Preconditions.checkArgument(
					transformData.startIndex[i - 1] + nameBinCountMap.get(
						transformData.selectedCols[i - 1]) == transformData.unscaledModel.length - 1,
					"Assembled vector size error!");
			}
		}

		public static double[] getModelData(LinearModelData linearModelData, ScorecardTransformData transformData) {
			double[] modelData = linearModelData.coefVector.getData();
			Preconditions.checkState(linearModelData.hasInterceptItem,
				"LinearModel in Scorecard not have intercept!");
			if (!transformData.withSelector) {
				Preconditions.checkState(
					!transformData.isWoe || modelData.length == transformData.selectedCols.length + 1,
					"SelectedCol length: " + transformData.selectedCols.length + "; Model efficients length: "
						+ modelData.length);
			}
			return modelData;
		}

		public static int findLinearModelCoefIdx(String featureColName,
												 Integer binIndex,
												 ScorecardTransformData transformData) {
			int result = 0;
			if (transformData.isWoe) {
				result = transformData.withSelector
					? TableUtil.findColIndex(transformData.stepwiseSelectedCols, featureColName)
					: transformData.featureIndex;
			} else {
				if (transformData.withSelector) {
					int idx = TableUtil.findColIndex(transformData.stepwiseSelectedCols, featureColName);
					result = idx == -1 ? -1 : getIdx(transformData, featureColName) + binIndex;
				} else {
					result = transformData.startIndex[transformData.featureIndex] + binIndex;
				}
			}
			return result;
		}

		private static int getIdx(ScorecardTransformData transformData, String featureColName) {
			int stepwiseIndex = TableUtil.findColIndex(transformData.stepwiseSelectedCols, featureColName);
			int startIndex = 0;
			for (int i = 0; i < stepwiseIndex; i++) {
				int idx = TableUtil.findColIndex(transformData.selectedCols, transformData.stepwiseSelectedCols[i]);
				if (idx < transformData.startIndex.length - 1) {
					startIndex += (transformData.startIndex[idx + 1] - transformData.startIndex[idx]);
				} else {
					startIndex += (transformData.unscaledModel.length - 1 - transformData.startIndex[
						transformData.startIndex.length - 1]);
				}
			}
			return startIndex;
		}

		public static Double getModelValue(int linearModelCoefIdx, Double woe, double[] efficients, boolean isWoe,
										   double defaultWoe) {
			if (isWoe) {
				Double val = getLinearCoef(efficients, linearModelCoefIdx);
				if (val == null) {
					return null;
				}
				if (null == woe) {
					return Double.isNaN(defaultWoe) ? null : val * defaultWoe;
				} else {
					return val * woe;
				}
			} else {
				return getLinearCoef(efficients, linearModelCoefIdx);
			}
		}

		//if intercept, it will + 1
		private static Double getLinearCoef(double[] efficients, int linearModelCoefIdx) {
			return linearModelCoefIdx < 0 ? null : efficients[linearModelCoefIdx + 1];
		}

	}

	public static class ScorecardTransformData implements Serializable {
		public double[] unscaledModel;
		public double[] scaledModel;
		public boolean isScaled;
		public boolean isWoe;
		public String[] selectedCols;
		public double defaultWoe;
		public int featureIndex;
		public int[] startIndex;

		//for stepwise
		public boolean withSelector;
		public LinearModelType linearModelType;
		public String[] stepwiseSelectedCols;

		public ScorecardTransformData(boolean isScaled, String[] selectedCols, boolean isWoe, double defaultWoe,
									  boolean withSelector) {
			this.isScaled = isScaled;
			this.selectedCols = selectedCols;
			this.isWoe = isWoe;
			this.defaultWoe = defaultWoe;
			this.withSelector = withSelector;
		}
	}

	private static DataSet <Tuple2 <String, Integer>> featureNameBinCount(
		DataSet <FeatureBinsCalculator> featureBorderDataSet) {
		return featureBorderDataSet.map(new MapFunction <FeatureBinsCalculator, Tuple2 <String, Integer>>() {
			private static final long serialVersionUID = 6330797694544909126L;

			@Override
			public Tuple2 <String, Integer> map(FeatureBinsCalculator value) throws Exception {
				return Tuple2.of(value.getFeatureName(), FeatureBinsUtil.getBinEncodeVectorSize(value));
			}
		});
	}

	private static BatchOperator binningModelToConstraint(DataSet <FeatureBinsCalculator> featureBorderDataSet,
														  String[] selectedCols,
														  long environmentId) {
		DataSet <Row> constraint = featureBorderDataSet.mapPartition(
			new MapPartitionFunction <FeatureBinsCalculator, Row>() {
				private static final long serialVersionUID = 6519391092676175709L;

				@Override
				public void mapPartition(Iterable <FeatureBinsCalculator> values, Collector <Row> out) {
					Map <String, Integer> featureNameBinCountMap = new HashMap <>();
					values.forEach(featureBorder -> featureNameBinCountMap
						.put(featureBorder.getFeatureName(), FeatureBinsUtil.getBinEncodeVectorSize(featureBorder)));
					ConstraintBetweenBins[] constraintBetweenBins = new ConstraintBetweenBins[selectedCols.length];
					for (int i = 0; i < selectedCols.length; i++) {
						Integer binCount = featureNameBinCountMap.get(selectedCols[i]);
						Preconditions.checkNotNull(binCount, "BinCount for %s is not set!", selectedCols[i]);
						constraintBetweenBins[i] = new ConstraintBetweenBins(selectedCols[i], binCount);
					}
					FeatureConstraint featureConstraint = new FeatureConstraint();
					featureConstraint.addBinConstraint(constraintBetweenBins);
					out.collect(Row.of(featureConstraint));
				}
			}).setParallelism(1);
		return new DataSetWrapperBatchOp(constraint,
			BinningTrainForScorecardBatchOp.CONSTRAINT_TABLESCHEMA.getFieldNames(),
			new TypeInformation <?>[] {TypeInformation.of(FeatureConstraint.class)}).setMLEnvironmentId(environmentId);
	}

	private static Params getBinningOutputParams(Params params, boolean withSelector) {
		String[] selectedCols = params.get(HasSelectedCols.SELECTED_COLS);
		Encode encode = params.get(ScorecardTrainParams.ENCODE);
		Params binningPredictParams = new Params()
			.set(HasMLEnvironmentId.ML_ENVIRONMENT_ID, params.get(HasMLEnvironmentId.ML_ENVIRONMENT_ID))
			.set(BinningPredictParams.SELECTED_COLS, selectedCols)
			.set(BinningPredictParams.DEFAULT_WOE, params.get(BinningPredictParams.DEFAULT_WOE))
			.set(BinningPredictParams.DROP_LAST, false)
			.set(BinningPredictParams.HANDLE_INVALID, HasHandleInvalid.HandleInvalid.KEEP);

		switch (encode) {
			case WOE:
				binningPredictParams.set(BinningPredictParams.ENCODE, HasEncode.Encode.WOE);
				break;
			case ASSEMBLED_VECTOR:
				if (withSelector) {
					binningPredictParams.set(BinningPredictParams.ENCODE, HasEncode.Encode.VECTOR);
				} else {
					binningPredictParams.set(BinningPredictParams.ENCODE, HasEncode.Encode.ASSEMBLED_VECTOR);
				}
				break;
			default: {
				throw new RuntimeException("Not support!");
			}
		}

		switch (encode) {
			case WOE: {
				String[] binningOutCols = new String[selectedCols.length];
				for (int i = 0; i < selectedCols.length; i++) {
					binningOutCols[i] = selectedCols[i] + BINNING_OUTPUT_COL;
				}
				params.set(BinningPredictParams.OUTPUT_COLS, binningOutCols).set(HasOutputCol.OUTPUT_COL, null);
				binningPredictParams.set(BinningPredictParams.OUTPUT_COLS, binningOutCols);
				break;
			}
			case ASSEMBLED_VECTOR: {
				if (withSelector) {
					String[] binningOutCols = new String[selectedCols.length];
					for (int i = 0; i < selectedCols.length; i++) {
						binningOutCols[i] = selectedCols[i] + BINNING_OUTPUT_COL;
					}
					params.set(BinningPredictParams.OUTPUT_COLS, binningOutCols).set(HasOutputCol.OUTPUT_COL, null);
					binningPredictParams.set(BinningPredictParams.OUTPUT_COLS, binningOutCols);
				} else {
					params.set(HasOutputCol.OUTPUT_COL, BINNING_OUTPUT_COL).set(BinningPredictParams.OUTPUT_COLS,
						null);
					binningPredictParams.set(BinningPredictParams.OUTPUT_COLS, new String[] {BINNING_OUTPUT_COL});
				}
				break;
			}
			default: {
				throw new RuntimeException("Not support!");
			}
		}
		return binningPredictParams;
	}

	private static class MapConstraints extends RichMapFunction <Row, Row> {
		private static final long serialVersionUID = -8118713814960689315L;
		FeatureConstraint udConstraint;
		Map <String, Boolean> withElse;

		public MapConstraints(Map <String, Boolean> withElse) {
			this.withElse = withElse;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			String consString =
				(String) ((Row) getRuntimeContext().getBroadcastVariable("udConstraint").get(0))
					.getField(0);
			this.udConstraint = FeatureConstraint.fromJson(consString);
		}

		@Override
		public Row map(Row value) throws Exception {
			FeatureConstraint binConstraint = (FeatureConstraint) value.getField(0);
			udConstraint.addDim(binConstraint);
			//no matter where are the constraint of else and null, they shall be passed with index of -1 or -2.
			udConstraint.modify(withElse);
			return Row.of(udConstraint);
		}
	}

	private static BinningModel setBinningModelData(BatchOperator <?> modelData, Params params) {
		BinningModel binningModel = new BinningModel(params);
		binningModel.setModelData(modelData);
		return binningModel;
	}

	private static ScoreModel setScoreModelData(
		BatchOperator <?> modelData, Params params, TypeInformation <?> labelType) {

		ScoreModel scoreM = new ScoreModel(params);
		scoreM.setModelData(modelData);
		return scoreM;
	}

	static LinearModelData scaleLinearModelWeight(LinearModelData modelData, Tuple2 <Double, Double> scaleInfo) {
		double[] efficients = modelData.coefVector.getData();
		double scaleA = scaleInfo.f0;
		double scaleB = scaleInfo.f1;
		efficients[0] = scaleWeight(efficients[0], scaleA, scaleB, true);
		for (int i = 1; i < efficients.length; i++) {
			efficients[i] = scaleWeight(efficients[i], scaleA, scaleB, false);
		}
		return modelData;
	}

	private static double scaleWeight(double weight, double scaleA, double scaleB, boolean isIntercept) {
		if (isIntercept) {
			return (weight - scaleB) / scaleA;
		} else {
			return weight / scaleA;
		}
	}

	static class LoadLinearModel implements MapPartitionFunction <Row, LinearModelData> {
		private static final long serialVersionUID = -2042421522639330680L;

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <LinearModelData> collector) {
			List <Row> list = new ArrayList <>();
			rows.forEach(list::add);
			collector.collect(new LinearModelDataConverter().load(list));
		}
	}

	static class ScaleLinearModel implements MapFunction <LinearModelData, LinearModelData> {
		private static final long serialVersionUID = 1506707086583262871L;
		private Params params;

		public ScaleLinearModel(Params params) {
			this.params = params;
		}

		@Override
		public LinearModelData map(LinearModelData modelData) {
			return scaleLinearModelWeight(modelData, loadScaleInfo(params));
		}
	}

	static class SerializeLinearModel implements FlatMapFunction <LinearModelData, Row> {
		private static final long serialVersionUID = -780782639893457512L;

		@Override
		public void flatMap(LinearModelData modelData, Collector <Row> collector) {
			new LinearModelDataConverter(modelData.labelType).save(modelData, collector);
		}
	}

	@Override
	public ScorecardModelInfoBatchOp getModelInfoBatchOp() {
		return new ScorecardModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
