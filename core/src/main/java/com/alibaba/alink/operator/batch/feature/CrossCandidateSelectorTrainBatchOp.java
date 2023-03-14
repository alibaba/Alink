package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureEvaluator;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureSet;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
import com.alibaba.alink.params.feature.CrossCandidateSelectorTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.alibaba.alink.operator.batch.feature.AutoCrossTrainBatchOp.AC_TRAIN_DATA;
import static com.alibaba.alink.operator.batch.feature.AutoCrossTrainBatchOp.SESSION_ID;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(PortType.MODEL)})
@NameCn("cross候选特征选择训练")
@NameEn("Cross Candidate Selector Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CrossCandidateSelector")
public class CrossCandidateSelectorTrainBatchOp
	extends BaseCrossTrainBatchOp <CrossCandidateSelectorTrainBatchOp>
	implements CrossCandidateSelectorTrainParams <CrossCandidateSelectorTrainBatchOp> {

	public CrossCandidateSelectorTrainBatchOp() {
		this(new Params());
	}

	public CrossCandidateSelectorTrainBatchOp(Params params) {
		super(params);
	}

	DataSet <Row> buildAcModelData(DataSet <Tuple3 <Double, Double, Vector>> trainData,
								   DataSet <int[]> featureSizeDataSet,
								   DataColumnsSaver dataColumnsSaver) {
		String[] numericalCols = dataColumnsSaver.numericalCols;
		String[] categoricalCols = dataColumnsSaver.categoricalCols;
		int[] numericalIndices = dataColumnsSaver.numericalIndices;

		DataSet <Tuple4 <int[], Double, double[], Integer>> candidateAndAuc = trainData
			.mapPartition(new CalcAucOfCandidate(categoricalCols, numericalIndices.length, getFeatureCandidates()))
			.withBroadcastSet(featureSizeDataSet, "featureSize")
			.withBroadcastSet(trainData, "barrier")
			.partitionByHash(3);

		DataSet <Row> acModel = candidateAndAuc
			.mapPartition(new FilterAuc(getCrossFeatureNumber(), oneHotVectorCol, numericalCols))
			.withBroadcastSet(featureSizeDataSet, "featureSize");
		return acModel;
	}

	@Override
	void buildSideOutput(OneHotTrainBatchOp oneHotModel, DataSet <Row> acModel, List <String> numericalCols,
						 long mlEnvId) {

	}

	private static class CalcAucOfCandidate
		extends RichMapPartitionFunction <Tuple3 <Double, Double, Vector>,
		Tuple4 <int[], Double, double[], Integer>> {
		private List <int[]> candidateIndices;
		private int[] featureSize;
		private int numTasks;
		private int numericalSize;

		CalcAucOfCandidate(String[] inputCols, int numericalSize, String[] featureCandidates) {
			this.numericalSize = numericalSize;
			candidateIndices = new ArrayList <>(featureCandidates.length);
			for (String stringCandidate : featureCandidates) {
				String[] candidate = stringCandidate.split(",");
				for (int i = 0; i < candidate.length; i++) {
					candidate[i] = candidate[i].trim();
				}
				candidateIndices.add(TableUtil.findColIndices(inputCols, candidate));
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			featureSize = (int[]) getRuntimeContext().getBroadcastVariable("featureSize").get(0);

		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> values,
								 Collector <Tuple4 <int[], Double, double[], Integer>> out)
			throws Exception {

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (candidateIndices.size() <= taskId) {
				return;
			}
			List <Tuple3 <Double, Double, Vector>> data = (List <Tuple3 <Double, Double, Vector>>)
				SessionSharedData.get(AC_TRAIN_DATA, SESSION_ID, taskId);

			FeatureEvaluator evaluator = new FeatureEvaluator(
				LinearModelType.LR,
				data,
				featureSize,
				null,
				0.8,
				false,
				1);

			for (int i = taskId; i < candidateIndices.size(); i += numTasks) {
				int[] candidate = candidateIndices.get(i);
				List <int[]> testingFeatures = new ArrayList <>();
				testingFeatures.add(candidate);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("evaluating " + JsonConverter.toJson(testingFeatures));
				}
				Tuple2 <Double, double[]> scoreCoef = evaluator.score(testingFeatures, numericalSize);
				double score = scoreCoef.f0;
				out.collect(Tuple4.of(candidate, score, scoreCoef.f1, 0));
			}

		}
	}

	private static class FilterAuc
		extends RichMapPartitionFunction <Tuple4 <int[], Double, double[], Integer>, Row> {

		private int crossFeatureNumber;
		private int[] indexSize;
		private String vectorCol;
		private String[] numericalCols;

		FilterAuc(int crossFeatureNumber, String vectorCol, String[] numericalCols) {
			this.crossFeatureNumber = crossFeatureNumber;
			this.vectorCol = vectorCol;
			this.numericalCols = numericalCols;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			indexSize = (int[]) getRuntimeContext().getBroadcastVariable("featureSize").get(0);
		}

		@Override
		public void mapPartition(Iterable <Tuple4 <int[], Double, double[], Integer>> values,
								 Collector <Row> out) throws Exception {
			List <Tuple4 <int[], Double, double[], Integer>> canAndAuc = new ArrayList <>();
			for (Tuple4 <int[], Double, double[], Integer> value : values) {
				canAndAuc.add(value);
			}
			if (canAndAuc.size() == 0) {
				return;
			}

			FeatureSet featureSet = new FeatureSet(indexSize);
			featureSet.numericalCols = numericalCols;
			featureSet.indexSize = indexSize;
			featureSet.vecColName = vectorCol;
			featureSet.hasDiscrete = true;
			canAndAuc.sort(new Comparator <Tuple4 <int[], Double, double[], Integer>>() {
				@Override
				public int compare(Tuple4 <int[], Double, double[], Integer> o1,
								   Tuple4 <int[], Double, double[], Integer> o2) {
					return -o1.f1.compareTo(o2.f1);
				}
			});

			for (int i = 0; i < this.crossFeatureNumber; i++) {
				featureSet.addOneCrossFeature(canAndAuc.get(i).f0, canAndAuc.get(i).f1);
			}
			out.collect(Row.of(0L, featureSet.toString(), null));
			for (int i = 0; i < featureSet.crossFeatureSet.size(); i++) {
				out.collect(Row.of((long) (i + 1), JsonConverter.toJson(featureSet.crossFeatureSet.get(i)),
					featureSet.scores.get(i)));
			}
		}
	}
}
