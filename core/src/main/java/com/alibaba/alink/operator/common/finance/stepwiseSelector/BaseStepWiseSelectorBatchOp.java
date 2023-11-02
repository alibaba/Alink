package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.finance.ScorecardTrainBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.dataproc.vector.VectorAssemblerMapper;
import com.alibaba.alink.operator.common.feature.SelectorModelData;
import com.alibaba.alink.operator.common.feature.SelectorModelDataConverter;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearRegressionSummary;
import com.alibaba.alink.operator.common.linear.LocalLinearModel;
import com.alibaba.alink.operator.common.linear.LogistRegressionSummary;
import com.alibaba.alink.operator.common.linear.ModelSummary;
import com.alibaba.alink.operator.common.linear.ModelSummaryHelper;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.params.finance.BaseStepwiseSelectorParams;
import com.alibaba.alink.params.finance.ConstrainedLogisticRegressionTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Internal
public class BaseStepWiseSelectorBatchOp extends BatchOperator <BaseStepWiseSelectorBatchOp>
	implements BaseStepwiseSelectorParams <BaseStepWiseSelectorBatchOp>, AlinkViz <BaseStepWiseSelectorBatchOp> {
	private static final long serialVersionUID = -1353820179732001005L;

	private static final String INNER_VECTOR_COL = "vec";
	private static final String INNER_LABLE_COL = "label";
	private BatchOperator in;

	private DataSet <Object> labels;

	private boolean hasConstraint;
	private DataSet <Row> constraintDataSet;

	private boolean hasVectorSizes;
	private DataSet <int[]> vectorSizes = null;

	//col and label after standard
	private String selectColNew;
	private String labelColNew;
	private int selectedColIdxNew;
	private int labelIdxNew;

	//origin
	private String selectedCol;
	private String[] selectedCols;

	private LinearModelType linearModelType;

	private boolean inScorcard;

	public BaseStepWiseSelectorBatchOp(Params params) {
		super(params);
	}

	@Override
	public BaseStepWiseSelectorBatchOp linkFrom(BatchOperator <?>... inputs) {
		if (inputs.length != 2 && inputs.length != 1) {
			throw new InvalidParameterException("input size must be one or two.");
		}

		this.linearModelType = getLinearModelType();
		inScorcard = getParams().get(ScorecardTrainBatchOp.IN_SCORECARD);

		this.in = inputs[0];
		BatchOperator constraint = null;
		if (inputs.length == 2) {
			constraint = inputs[1];
		}

		int labelIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getLabelCol());
		TypeInformation labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), getLabelCol());

		int[] forceSelectedColIndices = new int[0];
		if (getParams().contains(BaseStepwiseSelectorParams.FORCE_SELECTED_COLS)) {
			forceSelectedColIndices = getForceSelectedCols();
		}

		String positiveLabel = null;
		if (LinearModelType.LR == this.linearModelType || inScorcard) {
			if (this.getParams().contains(ConstrainedLogisticRegressionTrainParams.POS_LABEL_VAL_STR)) {
				positiveLabel = this.getParams().get(ConstrainedLogisticRegressionTrainParams.POS_LABEL_VAL_STR);
			}
		}

		standardConstraint(constraint);
		standardLabel(positiveLabel);
		transformToVector(labelIdx, labelType, linearModelType);

		//summary
		DataSet <Vector> summarizerData = in.getDataSet()
			.map(new ToVectorWithReservedCols(selectedColIdxNew, labelIdxNew));

		DataSet <BaseVectorSummarizer> summary = StatisticsHelper.summarizer(summarizerData, true);

		DataSet <Tuple2 <Vector, Row>> trainData = StatisticsHelper
			.transformToVector(in, null, selectColNew, new String[] {labelColNew});

		//train
		DataSet <Row> result = trainData
			.mapPartition(new StepWiseMapPartition(forceSelectedColIndices,
				getAlphaEntry(),
				getAlphaStay(),
				getLinearModelType(),
				getOptimMethod(),
				getStepWiseType(),
				getL1(),
				getL2(),
				hasVectorSizes,
				hasConstraint))
			.withBroadcastSet(summary, "summarizer")
			.withBroadcastSet(vectorSizes, "vectorSizes")
			.withBroadcastSet(constraintDataSet, "constraint")
			.setParallelism(1);

		if (getWithViz()) {
			writeVizData(result, getLinearModelType(), selectedCols, this.getVizDataWriter());
		}

		setOutput(result.flatMap(new BuildModel(selectedCol, selectedCols, getLinearModelType())).setParallelism(1),
			new SelectorModelDataConverter().getModelSchema());

		Table[] sideTables = new Table[2];

		//linear model
		DataSet <Row> linearModel;
		if (labels != null) {
			linearModel = result.flatMap(
				new BuildLinearModel(getLinearModelType(), selectedCols,
					TableUtil.findColTypes(inputs[0].getSchema(), selectedCols),
					getLabelCol(), labelType, positiveLabel, inScorcard))
				.withBroadcastSet(labels, "labelValues");
		} else {
			linearModel = result.flatMap(
				new BuildLinearModel(getLinearModelType(), selectedCols,
					TableUtil.findColTypes(inputs[0].getSchema(), selectedCols),
					getLabelCol(), labelType, positiveLabel, inScorcard));
		}

		sideTables[0] = DataSetConversionUtil.toTable(getMLEnvironmentId(),
			linearModel, new LinearModelDataConverter(labelType).getModelSchema());

		//statistics
		sideTables[1] = DataSetConversionUtil.toTable(getMLEnvironmentId(),
			result, new String[] {"result"}, new TypeInformation[] {Types.STRING});

		setSideOutputTables(sideTables);

		return this;
	}

	public DataSet <SelectorResult> getStepWiseSummary() {
		return getSideOutput(1).getDataSet()
			.map(new ToClassificationSelectorResult(this.linearModelType, getSelectedCols()));
	}

	private static class ToClassificationSelectorResult implements MapFunction <Row, SelectorResult> {
		private static final long serialVersionUID = 382487577293357907L;
		private String[] selectedCols;
		private LinearModelType linearModelType;

		ToClassificationSelectorResult(LinearModelType linearModelType, String[] selectedCols) {
			this.selectedCols = selectedCols;
			this.linearModelType = linearModelType;
		}

		@Override
		public SelectorResult map(Row row) throws Exception {
			SelectorResult result;
			if (LinearModelType.LR == linearModelType) {
				result = JsonConverter.fromJson((String) row.getField(0),
					ClassificationSelectorResult.class);
			} else {
				result = JsonConverter.fromJson((String) row.getField(0),
					RegressionSelectorResult.class);
			}
			result.selectedCols = BaseStepWiseSelectorBatchOp.getSCurSelectedCols(selectedCols, result
				.selectedIndices);
			return result;
		}
	}

	//deal with null
	private void standardConstraint(BatchOperator constraint) {
		this.hasConstraint = true;
		if (constraint == null) {
			this.constraintDataSet = MLEnvironmentFactory.get(in.getMLEnvironmentId())
				.getExecutionEnvironment().fromElements(new Row(0));
			this.hasConstraint = false;
		} else {
			this.constraintDataSet = constraint.getDataSet();
		}
	}

	//label value to double type
	private void standardLabel(String positiveLabel) {
		String labelCol = getLabelCol();
		if (getLinearModelType() == LinearModelType.LR || inScorcard) {
			Tuple2 <BatchOperator, DataSet <Object>> tuple2
				= ModelSummaryHelper.transformLrLabel(in, labelCol, positiveLabel, getMLEnvironmentId());
			this.in = tuple2.f0;
			this.labels = tuple2.f1;
		}
	}

	//calc vector size
	private void calcVectorSizes(int[] selectedColIndices, boolean isVector) {
		if (isVector) {
			vectorSizes = in.getDataSet()
				.mapPartition(new CalcVectorSize(selectedColIndices))
				.reduce(new ReduceFunction <int[]>() {
					private static final long serialVersionUID = -3014179424640804678L;

					@Override
					public int[] reduce(int[] left, int[] right) {
						int[] result = new int[left.length];
						for (int i = 0; i < left.length; i++) {
							result[i] = Math.max(left[i], right[i]);
						}
						return result;
					}
				});
			hasVectorSizes = true;
		} else {
			vectorSizes = MLEnvironmentFactory.get(in.getMLEnvironmentId())
				.getExecutionEnvironment().fromElements(new int[0]);
			hasVectorSizes = false;
		}
	}

	private void transformToVector(int labelIdx, TypeInformation labelType, LinearModelType linearModelType) {
		if (getParams().contains(BaseStepwiseSelectorParams.SELECTED_COL)) {
			selectedCol = getSelectedCol();
			if (selectedCol != null && !selectedCol.isEmpty()) {
				selectColNew = selectedCol;
				selectedColIdxNew = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), selectedCol);
			}
			calcVectorSizes(null, false);

			labelColNew = getLabelCol();
			labelIdxNew = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getLabelCol());
		}

		if (getParams().contains(BaseStepwiseSelectorParams.SELECTED_COLS)) {
			selectedCols = getSelectedCols();
			int[] selectedColsIdx = TableUtil.findColIndicesWithAssertAndHint(in.getColNames(), selectedCols);
			calcVectorSizes(selectedColsIdx, true);

			//labelType to double when lr
			TypeInformation[] resultTypes = new TypeInformation[2];
			resultTypes[0] = AlinkTypes.VECTOR;
			if (linearModelType == LinearModelType.LR || this.inScorcard) {
				resultTypes[1] = Types.DOUBLE;
			} else {
				resultTypes[1] = labelType;
			}

			//standard input
			in = BatchOperator.fromTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
				in.getDataSet()
					.map(new VectorAssemblerFunc(selectedColsIdx, labelIdx))
					.withBroadcastSet(vectorSizes, "vectorSizes"),
				new String[] {INNER_VECTOR_COL, INNER_LABLE_COL},
				resultTypes));

			selectColNew = INNER_VECTOR_COL;
			labelColNew = INNER_LABLE_COL;
			selectedColIdxNew = 0;
			labelIdxNew = 1;
		}
	}

	private static void writeVizData(DataSet <Row> summary,
									 LinearModelType linearModelType,
									 String[] selectedCols,
									 VizDataWriterInterface writer) {
		DataSet <Row> dummy = summary.flatMap(new ProcViz(linearModelType, selectedCols, writer))
			.setParallelism(1)
			.name("WriteStepWiseViz");
		DataSetUtil.linkDummySink(dummy);
	}

	private static class ProcViz implements FlatMapFunction <Row, Row> {
		private static final long serialVersionUID = -2466978695333617548L;
		private LinearModelType linearModelType;
		private String[] selectedCols;
		private VizDataWriterInterface writer;

		ProcViz(LinearModelType linearModelType, String[] selectedCols, VizDataWriterInterface writer) {
			this.linearModelType = linearModelType;
			this.selectedCols = selectedCols;
			this.writer = writer;
		}

		@Override
		public void flatMap(Row row, Collector <Row> collector) throws Exception {
			String result = (String) row.getField(0);

			String vizData;
			if (LinearModelType.LR == linearModelType) {
				ClassificationSelectorResult selector = JsonConverter.fromJson(result,
					ClassificationSelectorResult.class);
				selector.selectedCols = getSCurSelectedCols(selectedCols, selector.selectedIndices);
				vizData = selector.toVizData();
			} else {
				RegressionSelectorResult selector = JsonConverter.fromJson(result, RegressionSelectorResult.class);
				selector.selectedCols = getSCurSelectedCols(selectedCols, selector.selectedIndices);
				vizData = selector.toVizData();
			}

			writer.writeBatchData(0L, vizData, System.currentTimeMillis());
		}
	}

	static class StepWiseMapPartition extends RichMapPartitionFunction <Tuple2 <Vector, Row>, Row> {
		private static final long serialVersionUID = -3204445094879081660L;
		private int featureSize;

		private boolean hasVectorSizes;
		private int[] vectorSizes;

		private BaseVectorSummarizer summary;
		private List <Tuple3 <Double, Double, Vector>> trainData;

		//stepwise params
		private StepWiseType stepwiseType;
		private double alphaEntry;
		private double alphaStay;
		private int[] forceSelectedIndices;

		//optim params
		private com.alibaba.alink.operator.common.linear.LinearModelType linearModelType;
		private String optimMethod;
		private double l1;
		private double l2;
		private boolean hasConstraint;
		private FeatureConstraint constraints;

		StepWiseMapPartition(int[] forceSelectedIndices,
							 double alphaEntry,
							 double alphaStay,
							 LinearModelType linearModelType,
							 String optimMethod,
							 StepWiseType stepwiseType,
							 double l1,
							 double l2,
							 boolean hasVectorSizes,
							 boolean hasConstraint) {
			this.forceSelectedIndices = forceSelectedIndices;
			this.alphaEntry = alphaEntry;
			this.alphaStay = alphaStay;
			this.linearModelType = com.alibaba.alink.operator.common.linear.LinearModelType.valueOf(
				linearModelType.name());
			this.optimMethod = optimMethod.toUpperCase().trim();
			this.stepwiseType = stepwiseType;
			this.l1 = l1;
			this.l2 = l2;
			this.hasVectorSizes = hasVectorSizes;
			this.hasConstraint = hasConstraint;
		}

		public void open(Configuration conf) {
			this.summary = (BaseVectorSummarizer) this.getRuntimeContext().getBroadcastVariable("summarizer").get(0);

			if (hasVectorSizes) {
				List <Object> obj = this.getRuntimeContext().getBroadcastVariable("vectorSizes");
				int[] curVectorSizes = (int[]) obj.get(0);
				vectorSizes = new int[curVectorSizes.length + 1];
				vectorSizes[0] = 0;
				for (int i = 0; i < curVectorSizes.length; i++) {
					vectorSizes[i + 1] = vectorSizes[i] + curVectorSizes[i];
				}
			}

			String constraint = null;
			if (hasConstraint) {
				Object obj = ((Row) this.getRuntimeContext().getBroadcastVariable("constraint").get(0)).getField(0);

				if (obj instanceof FeatureConstraint) {
					constraint = obj.toString();
				} else {
					constraint = (String) obj;
				}
			}
			this.constraints = FeatureConstraint.fromJson(constraint);
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Vector, Row>> iterable, Collector <Row> collector) throws
			Exception {
			trainData = transformData(iterable);

			//set selected and rest
			List <Integer> selected = new ArrayList <>();
			List <Integer> selectedDummy = new ArrayList <>();
			List <Integer> rest = new ArrayList <>();

			for (int forceSelectedIdx : forceSelectedIndices) {
				selected.add(forceSelectedIdx);
				selectedDummy.addAll(getOne(forceSelectedIdx, vectorSizes));
			}

			for (int i = 0; i < featureSize; i++) {
				if (!selected.contains(i)) {
					rest.add(i);
				}
			}

			ModelSummary lrBest = null;
			double maxValue;
			List <SelectorStep> selectorSteps = new ArrayList <>();

			//trainLinear only has forceSelectedCol and intercept.
			if (forceSelectedIndices != null && forceSelectedIndices.length != 0) {
				lrBest = train(getIndicesFromList(selectedDummy));
			}

			//train with all features
			ModelSummary wholeModelSummary = null;
			if (isLinearRegression()) {
				wholeModelSummary = train(null);
			}

			while (true) {
				if (rest.isEmpty()) {
					break;
				}
				int selectedIndex = -1;
				ModelSummary lrr = null;

				//forward
				maxValue = Double.NEGATIVE_INFINITY;
				for (Integer aRest : rest) {
					List <Integer> curSelected = new ArrayList <>();
					curSelected.addAll(selectedDummy);
					curSelected.addAll(getOne(aRest, vectorSizes));

					ModelSummary lrCur = train(getIndicesFromList(curSelected));

					Tuple2 <Double, Double> forwardValues = getForwardValue(lrCur, lrBest, this.stepwiseType);

					//forward selector
					if (forwardValues.f0 > maxValue) {
						if (selected.size() == 0 || forwardValues.f1 <= alphaEntry) {
							maxValue = forwardValues.f0;
							lrr = lrCur;
							selectedIndex = aRest;
						}
					}
				}

				//forward step all variable p value >= alphaEntry, loop stop.
				if (selectedIndex < 0) {
					break;
				}

				//add var to selected
				selectedDummy.addAll(getOne(selectedIndex, vectorSizes));
				selected.add(selectedIndex);
				rest.remove(rest.indexOf(selectedIndex));

				//backward selector
				ArrayList <Integer> deleted = new ArrayList <>();
				if (selected.size() > 1) {
					double[] backwardValues = getBackwardValues(lrr, summary, this.stepwiseType, vectorSizes,
						selected);
					int maxBackWardIdx = argmax(backwardValues);
					if (backwardValues[maxBackWardIdx] >= alphaStay
						&& !isIdxExist(forceSelectedIndices, selected.get(maxBackWardIdx))) {
						int deleteIdx = selected.get(maxBackWardIdx);
						deleted.add(deleteIdx);
						selected.remove(selected.indexOf(deleteIdx));
						selectedDummy.removeAll(getOne(deleteIdx, vectorSizes));
						rest.add(deleteIdx);
					}
				}
				if (deleted.size() == 1 && deleted.get(0).equals(selectedIndex)) {
					break;
				}

				//cur best model
				lrBest = calMallowCp(lrr, wholeModelSummary);

				//add to selector steps
				selectorSteps.add(lrBest.toSelectStep(selectedIndex));
				for (Integer aDeleted : deleted) {
					int idx = aDeleted;
					for (int k = 0; k < selectorSteps.size(); k++) {
						if (idx == Integer.valueOf(selectorSteps.get(k).enterCol)) {
							selectorSteps.remove(k);
							break;
						}
					}
				}

				if (deleted.size() != 0) {
					lrBest = train(getIndicesFromList(selected));
				}
			}

			Row row = new Row(1);
			row.setField(0, bestModelResult(selectorSteps, lrBest));
			collector.collect(row);
		}

		private ModelSummary calMallowCp(ModelSummary lrBest, ModelSummary wholeSummary) {
			if (isLinearRegression()) {
				LinearRegressionSummary lrs = (LinearRegressionSummary) lrBest;
				//Cp = ( n - m - 1 )*( SSEp / SSEm )- n + 2*( p + 1 )
				lrs.mallowCp = (lrs.count - featureSize - 1) * (lrs.sse /
					((LinearRegressionSummary) wholeSummary).sse) - lrs.count + 2 * (lrs.lowerConfidence.length + 1);
			}
			return lrBest;
		}

		private boolean isLinearRegression() {
			return this.linearModelType == com.alibaba.alink.operator.common.linear.LinearModelType.LinearReg;
		}

		private String bestModelResult(List <SelectorStep> selectorSteps, ModelSummary bestSummary) {
			if (!isLinearRegression()) {
				ClassificationSelectorResult selector = new ClassificationSelectorResult();
				selector.entryVars = new ClassificationSelectorStep[selectorSteps.size()];
				selector.selectedIndices = new int[selectorSteps.size() + forceSelectedIndices.length];
				System.arraycopy(forceSelectedIndices, 0, selector.selectedIndices, 0, forceSelectedIndices.length);

				for (int i = 0; i < selectorSteps.size(); i++) {
					selector.entryVars[i] = (ClassificationSelectorStep) selectorSteps.get(i);
					selector.selectedIndices[i + forceSelectedIndices.length] = Integer.valueOf(
						selector.entryVars[i].enterCol);
				}
				selector.modelSummary = (LogistRegressionSummary) bestSummary;
				return JsonConverter.toJson(selector);
			} else {
				RegressionSelectorResult selector = new RegressionSelectorResult();
				selector.entryVars = new RegressionSelectorStep[selectorSteps.size()];
				selector.selectedIndices = new int[selectorSteps.size() + forceSelectedIndices.length];

				System.arraycopy(forceSelectedIndices, 0, selector.selectedIndices, 0, forceSelectedIndices.length);

				for (int i = 0; i < selectorSteps.size(); i++) {
					selector.entryVars[i] = (RegressionSelectorStep) selectorSteps.get(i);
					selector.selectedIndices[i + forceSelectedIndices.length] = Integer.valueOf(
						selector.entryVars[i].enterCol);
				}

				selector.modelSummary = (LinearRegressionSummary) bestSummary;
				return JsonConverter.toJson(selector);
			}
		}

		private List <Tuple3 <Double, Double, Vector>> transformData(Iterable <Tuple2 <Vector, Row>> iterable) {
			//transform data
			if (hasVectorSizes) {
				featureSize = vectorSizes.length - 1;
			} else {
				featureSize = summary.toSummary().vectorSize() - 1;
			}

			//f0: weight, f1: label, f2: data
			List <Tuple3 <Double, Double, Vector>> trainData = new ArrayList <>();
			for (Tuple2 <Vector, Row> tuple2 : iterable) {
				if (vectorSizes == null && tuple2.f0 instanceof SparseVector) {
					((SparseVector) tuple2.f0).setSize(featureSize);
				}
				trainData.add(Tuple3.of(1.0, ((Number) tuple2.f1.getField(0)).doubleValue(), tuple2.f0));
			}
			return trainData;
		}

		private ModelSummary train(int[] indices) {
			String constraint = indices == null ?
				constraints.toString() :
				constraints.extractConstraint(indices);
			return
				LocalLinearModel.trainWithSummary(trainData, indices,
					this.linearModelType, this.optimMethod,
					true, false,
					constraint, l1, l2, summary);
		}
	}

	public static String[] getSCurSelectedCols(String[] selectedCols, int[] indices) {
		if (selectedCols == null || selectedCols.length == 0) {
			return null;
		}

		String[] curSelectedCols = new String[indices.length];
		for (int i = 0; i < indices.length; i++) {
			curSelectedCols[i] = selectedCols[indices[i]];
		}

		return curSelectedCols;
	}

	/**
	 * build selector model.
	 */
	public static class BuildModel implements FlatMapFunction <Row, Row> {
		private static final long serialVersionUID = -4792429339624354557L;
		private String selectedCol;
		private String[] selectedCols;
		private LinearModelType linearModelType;

		BuildModel(String selectedCol, String[] selectedCols, LinearModelType linearModelType) {
			this.selectedCol = selectedCol;
			this.selectedCols = selectedCols;
			this.linearModelType = linearModelType;
		}

		@Override
		public void flatMap(Row row, Collector <Row> collector) throws Exception {
			SelectorModelData data = new SelectorModelData();

			if (LinearModelType.LR == linearModelType) {
				ClassificationSelectorResult result = JsonConverter.fromJson((String) row.getField(0),
					ClassificationSelectorResult.class);

				data.vectorColName = selectedCol;
				data.selectedIndices = result.selectedIndices;
				data.vectorColNames = getSCurSelectedCols(selectedCols, result.selectedIndices);

			} else {
				RegressionSelectorResult result = JsonConverter.fromJson((String) row.getField(0),
					RegressionSelectorResult.class);

				data.vectorColName = selectedCol;
				data.selectedIndices = result.selectedIndices;
				data.vectorColNames = getSCurSelectedCols(selectedCols, result.selectedIndices);

			}

			new SelectorModelDataConverter().save(data, collector);
		}
	}

	/**
	 * build linear model.
	 */
	public static class BuildLinearModel extends RichFlatMapFunction <Row, Row> {
		private LinearModelType linearModelType;
		private String[] selectedCols;
		private TypeInformation[] selectedColsType;
		private String labelCol;
		private TypeInformation labelType;
		private Object[] labelValues;
		private String positiveLabel;
		private boolean inScorecard;

		public BuildLinearModel(LinearModelType linearModelType,
								String[] selectedCols,
								TypeInformation[] selectColsTypes,
								String labelCol,
								TypeInformation labelType,
								String positiveLabel,
								boolean inScorecard) {
			this.linearModelType = linearModelType;
			this.selectedCols = selectedCols;
			this.selectedColsType = selectColsTypes;
			this.labelCol = labelCol;
			this.labelType = labelType;
			this.positiveLabel = positiveLabel;
			this.inScorecard = inScorecard;
		}

		public void open(Configuration conf) {
			if (LinearModelType.LR == this.linearModelType || inScorecard) {
				List <Object> labels = this.getRuntimeContext().getBroadcastVariable("labelValues");
				labelValues = ModelSummaryHelper.orderLabels(labels, positiveLabel);
			}
		}

		@Override
		public void flatMap(Row row, Collector <Row> out) throws Exception {
			LinearModelData modelData;
			DenseVector coefs;
			int[] selectedIndices;

			if (LinearModelType.LR == linearModelType) {
				ClassificationSelectorResult result = JsonConverter.fromJson((String) row.getField(0),
					ClassificationSelectorResult.class);
				selectedIndices = result.selectedIndices;
				coefs = result.modelSummary.beta;

			} else {
				RegressionSelectorResult result = JsonConverter.fromJson((String) row.getField(0),
					RegressionSelectorResult.class);
				selectedIndices = result.selectedIndices;
				coefs = result.modelSummary.beta;
			}

			String[] featureCols = getSCurSelectedCols(selectedCols, selectedIndices);
			String[] featureTypes = new String[featureCols.length];
			for (int i = 0; i < featureCols.length; i++) {
				featureTypes[i] = selectedColsType[selectedIndices[i]].getTypeClass().getSimpleName();
			}

			Params meta = new Params()
				.set(ModelParamName.MODEL_NAME, "model")
				.set(ModelParamName.LINEAR_MODEL_TYPE,
					com.alibaba.alink.operator.common.linear.LinearModelType.valueOf(linearModelType.name()))
				.set(ModelParamName.LABEL_VALUES, labelValues)
				.set(ModelParamName.HAS_INTERCEPT_ITEM, true)
				.set(ModelParamName.FEATURE_TYPES, featureTypes)
				.set(LinearTrainParams.LABEL_COL, labelCol);

			modelData = BaseLinearModelTrainBatchOp.buildLinearModelData(meta,
				featureCols,
				this.labelType,
				null,
				true,
				false,
				Tuple2.of(coefs, new double[] {0}));

			new LinearModelDataConverter(this.labelType).save(modelData, out);
		}
	}

	private static List <Integer> getOne(int selectedIdx, int[] vectorSizes) {
		ArrayList <Integer> result = new ArrayList <>();
		if (vectorSizes == null) {
			result.add(selectedIdx);
		} else {
			for (int i = vectorSizes[selectedIdx]; i < vectorSizes[selectedIdx + 1]; i++) {
				result.add(i);
			}
		}
		return result;
	}

	private static Tuple2 <Double, Double> getForwardValue(ModelSummary modelSummary, ModelSummary lastSummary,
														   StepWiseType type) {
		switch (type) {
			case fTest:
				LinearRegressionSummary linearRegressionSummary = (LinearRegressionSummary) modelSummary;
				return Tuple2.of(linearRegressionSummary.fValue, linearRegressionSummary.pValue);
			case scoreTest:
				LogistRegressionSummary logistRegressionSummary = (LogistRegressionSummary) modelSummary;
				return Tuple2.of(logistRegressionSummary.scoreChiSquareValue, logistRegressionSummary.scorePValue);
			case marginalContribution:
				double lastLoss = 0;
				if (lastSummary != null) {
					lastLoss = lastSummary.loss;
				}
				double mc = (modelSummary.loss - lastLoss) / modelSummary.count;
				return Tuple2.of(mc, mc);
			default:
				throw new RuntimeException("It is not support.");

		}

	}

	private static double[] getBackwardValues(ModelSummary modelSummary,
											  BaseVectorSummarizer srt,
											  StepWiseType type,
											  int[] vectorSizes,
											  List <Integer> selected) {
		switch (type) {
			case fTest:
				LinearRegressionSummary linearRegressionSummary = (LinearRegressionSummary) modelSummary;
				return linearRegressionSummary.tPVaues;
			case scoreTest:
				LogistRegressionSummary logistRegressionSummary = (LogistRegressionSummary) modelSummary;
				return Arrays.copyOfRange(logistRegressionSummary.waldPValues, 1,
					logistRegressionSummary.waldPValues.length);
			case marginalContribution:
				int featureNum = selected.size();
				//                if (vectorSizes != null) {
				//                    featureNum = vectorSizes.length - 1;
				//                }
				double[] mcs = new double[featureNum];

				for (int i = 0; i < featureNum; i++) {
					ArrayList <Integer> curSelectedDummay = new ArrayList <>();
					for (int j = 0; j < i; j++) {
						curSelectedDummay.addAll(getOne(selected.get(j), vectorSizes));
					}
					for (int j = i + 1; j < featureNum; j++) {
						curSelectedDummay.addAll(getOne(selected.get(j), vectorSizes));
					}

					com.alibaba.alink.operator.common.linear.LinearModelType linearModelType;

					if (modelSummary instanceof LogistRegressionSummary) {
						linearModelType = com.alibaba.alink.operator.common.linear.LinearModelType.LR;
					} else {
						linearModelType = com.alibaba.alink.operator.common.linear.LinearModelType.LinearReg;
					}

					Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model =
						Tuple4.of(modelSummary.beta, modelSummary.gradient, modelSummary.hessian, modelSummary.loss);
					ModelSummary lrCur =
						LocalLinearModel.calcModelSummary(model, srt, linearModelType,
							getIndicesFromList(curSelectedDummay));
					mcs[i] = (lrCur.loss - modelSummary.loss) / modelSummary.count;
				}
				return mcs;
			default:
				throw new RuntimeException("It is not support.");
		}

	}

	public static class ToVectorWithReservedCols implements MapFunction <Row, Vector> {
		private static final long serialVersionUID = 5163307315870607698L;
		private int vectorColIdx;
		private int labelColIdx;

		public ToVectorWithReservedCols(int vectorColIndex, int labelColIdx) {
			this.vectorColIdx = vectorColIndex;
			this.labelColIdx = labelColIdx;
		}

		@Override
		public Vector map(Row in) throws Exception {
			Vector vec = VectorUtil.getVector(in.getField(vectorColIdx));

			if (vec == null) {
				throw new RuntimeException(
					"vector is null, please check your input data.");
			}

			return vec.prefix(((Number) in.getField(labelColIdx)).doubleValue());
		}
	}

	private static class CalcVectorSize implements MapPartitionFunction <Row, int[]> {
		private static final long serialVersionUID = -4671985561279422505L;
		private int[] selectedColIndices;

		CalcVectorSize(int[] selectedColIndices) {
			this.selectedColIndices = selectedColIndices;
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <int[]> collector) throws Exception {
			int[] vectorSizes = new int[selectedColIndices.length];
			Arrays.fill(vectorSizes, 0);
			int curLocalSize = 0;
			for (Row row : iterable) {
				for (int i = 0; i < vectorSizes.length; i++) {
					Vector vec = VectorUtil.getVector(row.getField(selectedColIndices[i]));
					if (vec instanceof DenseVector) {
						curLocalSize = vec.size();
					} else {
						SparseVector sv = (SparseVector) vec;
						curLocalSize = sv.size() == -1 ? sv.numberOfValues() : sv.size();
					}
					vectorSizes[i] = Math.max(vectorSizes[i], curLocalSize);
				}
			}
			collector.collect(vectorSizes);
		}
	}

	public static class VectorAssemblerFunc extends RichMapFunction <Row, Row> {
		private static final long serialVersionUID = 2474917145545199423L;
		private int[] vectorSizes;
		private int[] selectedColIndices;
		private int labelColIdx;

		public VectorAssemblerFunc(int[] selectedColIndices, int labelColIdx) {
			this.selectedColIndices = selectedColIndices;
			this.labelColIdx = labelColIdx;

		}

		public void open(Configuration conf) {
			this.vectorSizes = (int[]) this.getRuntimeContext().getBroadcastVariable("vectorSizes").get(0);
		}

		@Override
		public Row map(Row row) throws Exception {
			int featureNum = selectedColIndices.length;
			Object[] values = new Object[featureNum];
			for (int i = 0; i < featureNum; i++) {
				Vector vec = VectorUtil.getVector(row.getField(selectedColIndices[i]));
				if (vec instanceof SparseVector) {
					((SparseVector) vec).setSize(vectorSizes[i]);
				}
				values[i] = vec;
			}

			Row out = new Row(2);
			out.setField(0, VectorAssemblerMapper.assembler(values));
			out.setField(1, row.getField(labelColIdx));

			return out;
		}

	}

	private static int[] getIndicesFromList(List <Integer> indices) {
		int[] result = new int[indices.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = indices.get(i);
		}
		return result;
	}

	private static int argmax(double[] values) {
		if (values == null && values.length == 0) {
			throw new RuntimeException("max values is null.");
		}
		int idx = 0;
		double maxVal = values[0];
		for (int i = 1; i < values.length; i++) {
			if (maxVal < values[i]) {
				maxVal = values[i];
				idx = i;
			}
		}
		return idx;
	}

	private static Boolean isIdxExist(int[] indices, int idx) {
		for (int i = 0; i < indices.length; i++) {
			if (idx == indices[i]) {
				return true;
			}
		}
		return false;
	}
}