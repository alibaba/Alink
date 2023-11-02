package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BinningPredictBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.common.feature.BinningModelDataConverter;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.finance.group.CalDirection;
import com.alibaba.alink.operator.common.finance.group.CalcGradient;
import com.alibaba.alink.operator.common.finance.group.CalcLosses;
import com.alibaba.alink.operator.common.finance.group.CalcStep;
import com.alibaba.alink.operator.common.finance.group.EvalData;
import com.alibaba.alink.operator.common.finance.group.GroupNode;
import com.alibaba.alink.operator.common.finance.group.GroupScoreCardVariable;
import com.alibaba.alink.operator.common.finance.group.GroupScoreModelConverter;
import com.alibaba.alink.operator.common.finance.group.OutputModel;
import com.alibaba.alink.operator.common.finance.group.SplitInfo;
import com.alibaba.alink.operator.common.finance.group.UpdateModel;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateCoefficient;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateConvergenceInfo;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateSkyk;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.params.feature.HasEncode;
import com.alibaba.alink.params.finance.GroupScorecardTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.feature.BinningModel;
import com.alibaba.alink.pipeline.finance.SimpleGroupScoreModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(PortType.DATA),
	@PortSpec(PortType.DATA)
})
@OutputPorts(values = {
	@PortSpec(PortType.DATA)
})

@ParamSelectColumnSpec(name = "selectCols", portIndices = 0)
@ParamSelectColumnSpec(name = "labelCol", portIndices = 0)
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES,
	portIndices = 0)
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)

@NameCn("分群评分卡训练")
@NameEn("Group Score Trainer")
public class GroupScorecardTrainBatchOp extends BatchOperator <GroupScorecardTrainBatchOp>
	implements GroupScorecardTrainParams <GroupScorecardTrainBatchOp>, AlinkViz <GroupScorecardTrainBatchOp> {

	public GroupScorecardTrainBatchOp(Params params) {
		super(params);
	}

	public GroupScorecardTrainBatchOp() {
		super(null);
	}

	@Override
	public GroupScorecardTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> data = inputs[0];
		BatchOperator <?> dataBinningModel = inputs[1];
		BatchOperator <?> groupBinningModel = inputs[2];

		MLEnvironment mlEnv = MLEnvironmentFactory.get(data.getMLEnvironmentId());
		List <TransformerBase <?>> finalModel = new ArrayList <>();

		// data preprocessing, transform data to binning indices.
		// label encode.
		String[] groupCols = getGroupCols();
		String[] featureCols = getSelectedCols();
		String labelCol = getLabelCol();
		String weightCol = getWeightCol();

		// binning group cols by index mode.
		String[] groupBinPredictCols = getNewColNames(groupCols, GroupScoreCardVariable.GROUP_BINNING_OUTPUT_COL);
		BatchOperator <?> groupBinningOp = new BinningPredictBatchOp()
			.setMLEnvironmentId(data.getMLEnvironmentId())
			.setSelectedCols(groupCols)
			.setEncode(HasEncode.Encode.INDEX)
			.setOutputCols(groupBinPredictCols);

		data = groupBinningOp.linkFrom(groupBinningModel, data);
		finalModel.add(setBinningModelData(groupBinningModel, groupBinningOp.getParams()));

		// binning feature cols by vector.
		BatchOperator <?> featureBinningOp = new BinningPredictBatchOp()
			.setMLEnvironmentId(data.getMLEnvironmentId())
			.setSelectedCols(featureCols)
			.setEncode(HasEncode.Encode.ASSEMBLED_VECTOR)
			.setOutputCols(GroupScoreCardVariable.FEATURE_BINNING_OUTPUT_COL);

		data = featureBinningOp.linkFrom(dataBinningModel, data);
		finalModel.add(setBinningModelData(dataBinningModel, featureBinningOp.getParams()));

		// label and data convert
		// < group cols(indices), <label, weight, feature vector>, predictProb >
		TableSchema dataSchema = data.getSchema();
		int labelColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema, labelCol);
		TypeInformation <?> labelType = TableUtil.findColType(dataSchema, labelCol);
		int weightColIdx = -1;
		if (null != weightCol) {
			weightColIdx = TableUtil.findColIndex(dataSchema, weightCol);
		}
		int vectorColIdx = TableUtil.findColIndex(dataSchema, GroupScoreCardVariable.FEATURE_BINNING_OUTPUT_COL);
		int[] groupColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, groupBinPredictCols);

		DataSet <Object> labelsValues = getLabelValues(data.getDataSet(), labelColIdx);

		// ? if need intercept
		DataSet <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> trainData =
			data.getDataSet()
				.map(new PrepareTrainData(groupColIndices, vectorColIdx, weightColIdx, labelColIdx))
				.withBroadcastSet(labelsValues, GroupScoreCardVariable.LABEL_VALUES);

		// init model.
		Tuple2 <DataSet <Integer>, DataSet <DenseVector>> coefDimAndInitModel = getCoefDimAndInitModel(trainData);
		DataSet <Integer> coefficientDim = coefDimAndInitModel.f0;
		DataSet <DenseVector> coefficientVec = coefDimAndInitModel.f1;

		DataSet <OptimObjFunc> objFunc = mlEnv.getExecutionEnvironment()
			.fromElements(
				OptimObjFunc.getObjFunction(com.alibaba.alink.operator.common.linear.LinearModelType.LR, getParams()));

		// iteration begin.
		int minSamplesPerLeaf = getMinSamplesPerLeaf();
		int minOutcomeSamplesPerLeaf = getMinOutcomeSamplesPerLeaf();
		int maxLeaves = getMaxLeaves();

		System.out.println("minSamplesPerLeaf： " + minSamplesPerLeaf);

		TreeMeasure treeMeasure = getTreeSplitMeasure();
		int maxIter = 10;
		int numSearchStep = 100;

		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.trainData, trainData)
			.initWithBroadcastData(OptimVariable.model, coefficientVec)
			.initWithBroadcastData(OptimVariable.objFunc, objFunc)
			.initWithBroadcastData(GroupScoreCardVariable.GROUP_BINNING_MODLE, groupBinningModel.getDataSet())
			.initWithBroadcastData(GroupScoreCardVariable.LABEL_VALUES, labelsValues)
			.add(new PreallocateCoefficient(OptimVariable.currentCoef))
			.add(new PreallocateCoefficient(OptimVariable.minCoef))
			.add(new PreallocateConvergenceInfo(OptimVariable.convergenceInfo, 1))
			.add(new PreallocateVector(OptimVariable.dir, new double[] {0.0, OptimVariable.learningRate}))
			.add(new PreallocateVector(OptimVariable.grad))
			.add(new PreallocateSkyk(OptimVariable.numCorrections))
			.add(new GetAllSplitInfoAndStat(groupCols))
			.add(new AllReduce(GroupScoreCardVariable.SPLIT_DATA_CNT))
			.add(new GetSelectedDataIndices(minSamplesPerLeaf))
			.add(new CalcGradient())
			.add(new AllReduce(OptimVariable.gradAllReduce))
			.add(new CalDirection(OptimVariable.numCorrections))
			.add(new CalcLosses(LinearTrainParams.OptimMethod.LBFGS, numSearchStep))
			.add(new AllReduce(OptimVariable.lossAllReduce))
			.add(new UpdateModel(new Params(), OptimVariable.grad, LinearTrainParams.OptimMethod.LBFGS, numSearchStep))
			// Determine convergence, pred and eval when convergence.
			.add(new LrIterTerminateAndPred())
			.add(new AllReduce(GroupScoreCardVariable.EVAL_DATA_INFO))
			// calc gain
			.add(new EvalCalc())
			.setCompareCriterionOfNode0(new IterTermination())
			.closeWith(new OutputModel())
			.exec();

		BatchOperator <?> treeSCModel = new TableSourceBatchOp(
			DataSetConversionUtil.toTable(data.getMLEnvironmentId(), model,
				new GroupScoreModelConverter(labelType).getModelSchema()))
			.setMLEnvironmentId(data.getMLEnvironmentId());


		Params groupScorePredictModelParams = new Params();
		finalModel.add(setScoreModelData(treeSCModel, groupBinningOp.getParams()));

		BatchOperator <?> savedModel = new PipelineModel(finalModel.toArray(new TransformerBase[0])).save();

		DataSet <Row> modelRows = savedModel
			.getDataSet()
			.map(new PipelineModelMapper.ExtendPipelineModelRow(featureCols.length + 1));

		TypeInformation <?>[] selectedTypes = new TypeInformation[featureCols.length];
		Arrays.fill(selectedTypes, labelType);

		TableSchema modelSchema = PipelineModelMapper.getExtendModelSchema(
			savedModel.getSchema(), featureCols, selectedTypes);

		this.setOutput(modelRows, modelSchema);

		return this;
	}

	private static SimpleGroupScoreModel setScoreModelData(
		BatchOperator <?> modelData, Params params) {
		SimpleGroupScoreModel scoreM = new SimpleGroupScoreModel(params);
		scoreM.setModelData(modelData);
		return scoreM;
	}

	private Tuple2 <DataSet <Integer>, DataSet <DenseVector>> getCoefDimAndInitModel(
		DataSet <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> trainData) {
		DataSet <Tuple2 <Integer, DenseVector>> re = trainData
			.first(1)
			.map(
				new MapFunction <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>, Tuple2 <Integer, DenseVector>>() {
					@Override
					public Tuple2 <Integer, DenseVector> map(
						Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double> value) throws Exception {
						int vectorDim = value.f1.f2.size() + 1;
						DenseVector denseVector = new DenseVector(vectorDim);
						for (int i = 0; i < denseVector.size(); i++) {
							denseVector.set(i, 0.0);
						}
						denseVector.set(0, 1.0e-3);
						return Tuple2.of(vectorDim, denseVector);
					}
				});

		return Tuple2.of(re.map(new MapFunction <Tuple2 <Integer, DenseVector>, Integer>() {
				@Override
				public Integer map(Tuple2 <Integer, DenseVector> value) throws Exception {
					return value.f0;
				}
			}),
			re.map(new MapFunction <Tuple2 <Integer, DenseVector>, DenseVector>() {
				@Override
				public DenseVector map(Tuple2 <Integer, DenseVector> value) throws Exception {
					return value.f1;
				}
			}));
	}

	private static BinningModel setBinningModelData(BatchOperator <?> modelData, Params params) {
		BinningModel binningModel = new BinningModel(params);
		binningModel.setModelData(modelData);
		return binningModel;
	}

	private static DataSet <Object> getLabelValues(DataSet <Row> data, int labelIdx) {
		DataSet <Object> labelValues = data
			.distinct(labelIdx)
			.map(new MapFunction <Row, Object>() {
				@Override
				public Object map(Row row) throws Exception {
					return row.getField(labelIdx);
				}
			});
		return labelValues;
	}

	private static Object[] orderLabels(List <Object> unorderedLabelRows) {
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

	// < group cols(indices), <label, weight, feature vector>, predictProb>
	private static class PrepareTrainData
		extends RichMapFunction <Row, Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> {
		private int[] groupColIndices;

		private int vectorColIdx;
		private int weightColIdx;
		private int labelColIdx;

		private Object[] labelValues;

		public PrepareTrainData(int[] groupColIndices, int vectorColIdx, int weightColIdx, int labelColIdx) {
			this.groupColIndices = groupColIndices;
			this.vectorColIdx = vectorColIdx;
			this.weightColIdx = weightColIdx;
			this.labelColIdx = labelColIdx;
		}

		@Override
		public void open(Configuration parameters) {
			this.labelValues = orderLabels(
				getRuntimeContext().getBroadcastVariable(GroupScoreCardVariable.LABEL_VALUES));
		}

		@Override
		public Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double> map(Row value)
			throws Exception {
			//< group cols(indices), <label, weight, feature vector>, predictProb>

			Row groupColRow = Row.project(value, this.groupColIndices);
			double label = value.getField(labelColIdx).equals(labelValues[0]) ? 1.0 : -1.0;
			double weight = 1.0;
			if (weightColIdx != -1) {
				weight = ((Number) value.getField(weightColIdx)).doubleValue();
			}
			Vector vector = (Vector) value.getField(vectorColIdx);
			Double predictProb = -1.0;

			return Tuple3.of(groupColRow, Tuple3.of(weight, label, vector), predictProb);
		}
	}

	public static String[] getNewColNames(String[] colsNames, String prefix) {
		return Arrays
			.stream(colsNames)
			.map(col -> col + prefix)
			.toArray(String[]::new);
	}

	// eval and pred
	static public class EvalCalc extends ComputeFunction {
		public EvalCalc() {

		}

		@Override
		public void calc(ComContext context) {
			CalcStep calcStep = context.getObj(GroupScoreCardVariable.NEXT_STEP);
			if (CalcStep.EVAL == calcStep) {
				double[] evalInfo = context.getObj(GroupScoreCardVariable.EVAL_DATA_INFO);
				double gain = 1.0;
				int splitInfoIndex = context.getObj(GroupScoreCardVariable.SPLIT_INFO_INDEX);
				List <SplitInfo> splitInfos = context.getObj(GroupScoreCardVariable.SPLIT_INFO);

				List <GroupNode> allNodes = context.getObj(GroupScoreCardVariable.GROUP_ALL_NODES);
				int curNodeId = context.getObj(GroupScoreCardVariable.CUR_NODE_ID);
				GroupNode curNode = allNodes.get(curNodeId);

				if (splitInfoIndex == -1) {
					// for all data;
					//System.out.println("all: " + "gain:" + gain);
					curNode.gain = gain;
					curNode.isCalculated = true;
					curNode.isLeaf = false;
					context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.SPLIT);
				} else {
					//System.out.println(
					//	"splitInfos.size:" + splitInfos.size() + " splitInfoIndex: " + splitInfoIndex + " gain: "
					//		+ gain);
					SplitInfo info = splitInfos.get(splitInfoIndex);
					info.gain = gain;
					info.calculated = true;
					splitInfoIndex++;
					context.putObj(GroupScoreCardVariable.SPLIT_INFO_INDEX, splitInfoIndex);

					if (splitInfoIndex == splitInfos.size()) {
						SplitInfo bestSplitInfo = splitInfos.get(0);
						// get best gain
						for (SplitInfo splitInfo : splitInfos) {
							if (bestSplitInfo.gain < splitInfo.gain) {
								bestSplitInfo = splitInfo;
							}
						}

						curNode.split = bestSplitInfo;
						curNode.isCalculated = true;
						curNode.leftNode = new GroupNode();
						curNode.leftNode.isCalculated = false;
						curNode.rightNode = new GroupNode();
						curNode.rightNode.isCalculated = false;
						curNode.evalData = new EvalData();
						curNode.evalData.ks = bestSplitInfo.gain;
						curNode.gain = bestSplitInfo.gain;

						{
							List <Integer> selectedIndices = context.getObj(
								GroupScoreCardVariable.SELECTED_DATA_INDICES);
							List <Integer> leftSelectedIndices = new ArrayList <>();
							List <Integer> rightSelectedIndices = new ArrayList <>();
							List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> data
								= context.getObj(OptimVariable.trainData);
							int splitValueIdx = bestSplitInfo.splitValueIdx;
							int splitColIdx = bestSplitInfo.splitColIdx;
							for (int idx : selectedIndices) {
								int valueIdx = ((Number) data.get(idx).f0.getField(splitColIdx)).intValue();
								if (info.isStr) {
									if (splitValueIdx == valueIdx) {
										leftSelectedIndices.add(idx);
									} else {
										rightSelectedIndices.add(idx);
									}
								} else {
									if (splitValueIdx <= valueIdx) {
										leftSelectedIndices.add(idx);
									} else {
										rightSelectedIndices.add(idx);
									}
								}
							}
							curNode.leftNode.selectedDataIndices = leftSelectedIndices;
							curNode.rightNode.selectedDataIndices = rightSelectedIndices;
						}

						curNode.leftNode.nodeId = allNodes.size();
						curNode.rightNode.nodeId = allNodes.size() + 1;

						allNodes.add(curNode.leftNode);
						allNodes.add(curNode.rightNode);

						// pred use best split info
						DenseVector leftCoefVector = new DenseVector(bestSplitInfo.leftWeights);
						DenseVector rightCoefVector = new DenseVector(bestSplitInfo.rightWeights);

						List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> data
							= context.getObj(OptimVariable.trainData);
						for (int idx : curNode.leftNode.selectedDataIndices) {
							Vector vector = data.get(idx).f1.f2;
							double dotValue = FeatureLabelUtil.dot(vector, leftCoefVector);
							data.get(idx).f2 = 1 - 1.0 / (1.0 + Math.exp(dotValue));
						}

						for (int idx : curNode.rightNode.selectedDataIndices) {
							Vector vector = data.get(idx).f1.f2;
							double dotValue = FeatureLabelUtil.dot(vector, rightCoefVector);
							data.get(idx).f2 = 1 - 1.0 / (1.0 + Math.exp(dotValue));
						}

						context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.SPLIT);
					}

				}
			}
		}
	}

	public static class IterTermination extends CompareCriterionFunction {

		@Override
		public boolean calc(ComContext context) {
			CalcStep step = context.getObj(GroupScoreCardVariable.NEXT_STEP);
			return step == CalcStep.Terminate;
		}
	}

	// lr terminate, pred and eval.

	static public class LrIterTerminateAndPred extends ComputeFunction {
		public LrIterTerminateAndPred() {

		}

		@Override
		public void calc(ComContext context) {
			//System.out.println("LrIterTerminateAndPred: " + context.getStepNo());
			if (CalcStep.LR_TRAIN_LEFT != context.getObj(GroupScoreCardVariable.NEXT_STEP) &&
				CalcStep.LR_TRAIN_RIGHT != context.getObj(GroupScoreCardVariable.NEXT_STEP)) {
				return;
			}

			if (context.getStepNo() == 1) {
				// init for all reduce
				double[] evalBinData = new double[100 * 2];
				context.putObj(GroupScoreCardVariable.EVAL_DATA_INFO, evalBinData);
			}

			CalcStep calcStep = context.getObj(GroupScoreCardVariable.NEXT_STEP);
			if (CalcStep.Terminate == calcStep) {
				return;
			}
			Tuple2 <DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
			Tuple2 <DenseVector, double[]> minCoef = context.getObj(OptimVariable.minCoef);
			//System.out.println("****: " + dir.f1[0]);
			if (dir.f1[0] < 0.0) {
				//System.out.println("-------lr finished--------" + context.getObj(NEXT_STEP));
				// get model
				DenseVector coefVector = minCoef.f0;

				// set to split info
				int splitInfoIndex = context.getObj(GroupScoreCardVariable.SPLIT_INFO_INDEX);
				if (splitInfoIndex != -1) {
					List <SplitInfo> splitInfoList = context.getObj(GroupScoreCardVariable.SPLIT_INFO);
					SplitInfo splitInfo = splitInfoList.get(splitInfoIndex);

					if (CalcStep.LR_TRAIN_LEFT == context.getObj(GroupScoreCardVariable.NEXT_STEP)) {
						splitInfo.leftWeights = coefVector.getData();
					} else {
						splitInfo.rightWeights = coefVector.getData();
					}
				}

				List <Integer> selectDataIndices = context.getObj(GroupScoreCardVariable.SELECTED_DATA_INDICES);
				List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> data
					= context.getObj(OptimVariable.trainData);
				for (int idx : selectDataIndices) {
					Vector vector = data.get(idx).f1.f2;
					double dotValue = FeatureLabelUtil.dot(vector, coefVector);
					double prob = 1 - 1.0 / (1.0 + Math.exp(dotValue));
					data.get(idx).f2 = prob;
				}

				// if right node is pred, then begin eval.
				if (CalcStep.LR_TRAIN_LEFT == calcStep) {
					context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.LR_TRAIN_RIGHT);
				}

				if (CalcStep.LR_TRAIN_RIGHT == calcStep) {
					// binning into 100
					// num_local_pos_count, num_local_neg_count;
					double[] evalBinData = new double[100 * 2];
					context.putObj(GroupScoreCardVariable.EVAL_DATA_INFO, evalBinData);
					context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.EVAL);
				}

			}
		}
	}

	/**
	 * get selected data indices from split info
	 */
	static public class GetAllSplitInfoAndStat extends ComputeFunction {

		private String[] groupColNames;

		public GetAllSplitInfoAndStat(String[] groupColNames) {
			this.groupColNames = groupColNames;
		}

		@Override
		public void calc(ComContext context) {
			//System.out.println("GetSelectedDataIndices: " + context.getStepNo());

			List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> data
				= context.getObj(OptimVariable.trainData);

			if (context.getStepNo() == 1) {
				GroupNode rootNode = new GroupNode();
				rootNode.nodeId = 0;

				List <Integer> selectedIndices = new ArrayList <>();
				for (int i = 0; i < data.size(); i++) {
					selectedIndices.add(i);
				}
				rootNode.selectedDataIndices = selectedIndices;

				context.putObj(GroupScoreCardVariable.SELECTED_DATA_INDICES, selectedIndices);

				context.putObj(GroupScoreCardVariable.SPLIT_INFO_INDEX, -1);
				context.putObj(GroupScoreCardVariable.SPLIT_INFO, new ArrayList <>());
				context.putObj(GroupScoreCardVariable.SPLIT_DATA_CNT, new double[1]);

				List <GroupNode> allNodes = new ArrayList <>();
				allNodes.add(rootNode.clone());
				context.putObj(GroupScoreCardVariable.GROUP_ALL_NODES, allNodes);
				context.putObj(GroupScoreCardVariable.CUR_NODE_ID, 0);

				context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.LR_TRAIN_RIGHT);

			} else {
				CalcStep step = context.getObj(GroupScoreCardVariable.NEXT_STEP);
				if (CalcStep.Terminate == step) {
					return;
				}

				List <GroupNode> allNodes = context.getObj(GroupScoreCardVariable.GROUP_ALL_NODES);
				int curNodeId = context.getObj(GroupScoreCardVariable.CUR_NODE_ID);
				GroupNode curNode = allNodes.get(curNodeId);

				if (CalcStep.SPLIT == step) {
					List <Row> groupBinningModel = context.getObj(GroupScoreCardVariable.GROUP_BINNING_MODLE);
					List <FeatureBinsCalculator> groupColsBins = new BinningModelDataConverter().load(
						groupBinningModel);

					if (-1 == (int) context.getObj(GroupScoreCardVariable.SPLIT_INFO_INDEX)) {

					} else {
						curNode.isCalculated = true;
						// recompute split info;
						curNode = findNextSplitNode(curNode, allNodes);
						if (curNode == null) {
							context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.Terminate);
							return;
						}
						context.putObj(GroupScoreCardVariable.CUR_NODE_ID, curNode.nodeId);
					}
					if (curNode == null) {
						context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.Terminate);
						return;
					}

					List <SplitInfo> splitInfos = new ArrayList <>();
					for (int i = 0; i < groupColNames.length; i++) {
						String groupColName = groupColNames[i];
						for (FeatureBinsCalculator bins : groupColsBins) {
							String featureName = bins.getFeatureName();
							if (featureName.equals(groupColName)) {
								int binCount = bins.getBinCount();
								for (int iBin = 0; iBin < binCount; iBin++) {
									SplitInfo splitInfo = new SplitInfo();
									splitInfo.splitColIdx = i;
									splitInfo.splitValueIdx = iBin;
									splitInfo.isStr = false;
									splitInfo.calculated = false;
									splitInfos.add(splitInfo);
								}
							} else {
								int binCount = bins.getBinCount();
								for (int iBin = 0; iBin < binCount; iBin++) {
									SplitInfo splitInfo = new SplitInfo();
									splitInfo.splitColIdx = i;
									splitInfo.splitValueIdx = iBin;
									splitInfo.isStr = true;
									splitInfos.add(splitInfo);
									splitInfo.calculated = false;
								}
							}
						}
					}

					curNode.splitInfos = splitInfos;

					context.putObj(GroupScoreCardVariable.SPLIT_INFO, splitInfos);
					context.putObj(GroupScoreCardVariable.SPLIT_INFO_INDEX, 0);

					double[] counts = new double[splitInfos.size() * 2];
					int infoIdx = 0;

					for (SplitInfo info : splitInfos) {
						List <Integer> selectedIndices = curNode.selectedDataIndices;
						List <Integer> leftSelectedIndices = new ArrayList <>();
						List <Integer> rightSelectedIndices = new ArrayList <>();

						int splitValueIdx = info.splitValueIdx;
						int splitColIdx = info.splitColIdx;
						for (int idx : selectedIndices) {
							int valueIdx = ((Number) data.get(idx).f0.getField(splitColIdx)).intValue();
							if (info.isStr) {
								if (splitValueIdx == valueIdx) {
									leftSelectedIndices.add(idx);
								} else {
									rightSelectedIndices.add(idx);
								}
							} else {
								if (splitValueIdx <= valueIdx) {
									leftSelectedIndices.add(idx);
								} else {
									rightSelectedIndices.add(idx);
								}
							}
						}

						counts[2 * infoIdx] = leftSelectedIndices.size();
						counts[2 * infoIdx + 1] = rightSelectedIndices.size();

						infoIdx++;
					}

					context.putObj(GroupScoreCardVariable.SPLIT_DATA_CNT, counts);
				}

			}
		}

	}

	/**
	 * get selected data indices from split info
	 */
	static public class GetSelectedDataIndices extends ComputeFunction {
		private int minSamplesPerLeaf;

		public GetSelectedDataIndices(int minSamplesPerLeaf) {
			this.minSamplesPerLeaf = minSamplesPerLeaf;
		}

		@Override
		public void calc(ComContext context) {
			//System.out.println("GetSelectedDataIndices: " + context.getStepNo());

			List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> data
				= context.getObj(OptimVariable.trainData);

			if (context.getStepNo() == 1) {

			} else {
				CalcStep step = context.getObj(GroupScoreCardVariable.NEXT_STEP);
				if (CalcStep.Terminate == step) {
					return;
				}

				List <GroupNode> allNodes = context.getObj(GroupScoreCardVariable.GROUP_ALL_NODES);
				int curNodeId = context.getObj(GroupScoreCardVariable.CUR_NODE_ID);
				GroupNode curNode = allNodes.get(curNodeId);

				List <SplitInfo> splitInfos = context.getObj(GroupScoreCardVariable.SPLIT_INFO);

				// re split.
				if (CalcStep.SPLIT == step) {
					//System.out.println("curNodeId: " + curNodeId + " need split");

					double[] counts = context.getObj(GroupScoreCardVariable.SPLIT_DATA_CNT);
					List <SplitInfo> splitInfosNew = new ArrayList <>();
					for (int i = 0; i < splitInfos.size(); i++) {
						if (counts[2 * i] >= minSamplesPerLeaf && counts[2 * i + 1] >= minSamplesPerLeaf) {
							splitInfos.get(i).leftDataCount = (long) counts[2 * i];
							splitInfos.get(i).rightDataCount = (long) counts[2 * i + 1];
							splitInfosNew.add(splitInfos.get(i));
						}
					}

					if (splitInfosNew.isEmpty()) {
						curNode.isLeaf = true;
						return;
					}

					splitInfos = splitInfosNew;
					curNode.splitInfos = splitInfos;

					context.putObj(GroupScoreCardVariable.SPLIT_INFO, splitInfos);
					context.putObj(GroupScoreCardVariable.SPLIT_INFO_INDEX, 0);

					//System.out.println("curNodeId: " + curNode.nodeId + " splitInfos: " + splitInfos.size());
				}

				//System.out.println("get select indices: " + context.getStepNo());
				int splitInfoIndex = context.getObj(GroupScoreCardVariable.SPLIT_INFO_INDEX);
				if (-1 == splitInfoIndex) {
					return;
				}
				splitInfos = context.getObj(GroupScoreCardVariable.SPLIT_INFO);
				SplitInfo info = splitInfos.get(splitInfoIndex);

				List <Integer> selectedIndices = curNode.selectedDataIndices;
				//System.out.println("selectedIndices: " + selectedIndices.size());
				List <Integer> leftSelectedIndices = new ArrayList <>();
				List <Integer> rightSelectedIndices = new ArrayList <>();

				int splitValueIdx = info.splitValueIdx;
				int splitColIdx = info.splitColIdx;
				for (int idx : selectedIndices) {
					// data
					int splitVal = ((Number) data.get(idx).f0.getField(splitColIdx)).intValue();
					if (info.isStr) {
						if (splitValueIdx == splitVal) {
							leftSelectedIndices.add(idx);
						} else {
							rightSelectedIndices.add(idx);
						}

					} else {
						if (splitValueIdx <= splitVal) {
							leftSelectedIndices.add(idx);
						} else {
							rightSelectedIndices.add(idx);
						}
					}
				}

				if (curNode.leftNode == null) {
					curNode.leftNode = new GroupNode();
				}
				curNode.leftNode.selectedDataIndices = leftSelectedIndices;

				if (curNode.rightNode == null) {
					curNode.rightNode = new GroupNode();
				}
				curNode.rightNode.selectedDataIndices = rightSelectedIndices;

				if (info.leftWeights == null) {
					context.putObj(GroupScoreCardVariable.SELECTED_DATA_INDICES, leftSelectedIndices);
					context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.LR_TRAIN_LEFT);
				} else {
					context.putObj(GroupScoreCardVariable.SELECTED_DATA_INDICES, rightSelectedIndices);
					context.putObj(GroupScoreCardVariable.NEXT_STEP, CalcStep.LR_TRAIN_RIGHT);
				}

				context.putObj(GroupScoreCardVariable.SPLIT_INFO_INDEX, splitInfoIndex);
			}
		}

	}

	private static GroupNode findNextSplitNode(GroupNode curNode, List <GroupNode> nodes) {
		int curNodeId = curNode.nodeId;
		for (int i = 0; i < nodes.size(); i++) {
			if (nodes.get(i).nodeId == curNodeId) {
				int j = i + 1;
				while (j < nodes.size()) {
					if (!nodes.get(j).isLeaf) {
						return nodes.get(j);
					}
					j++;
				}
				break;
			}
		}
		return null;
	}

}
