package com.alibaba.alink.operator.local.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

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
import com.alibaba.alink.params.feature.CrossCandidateSelectorTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(PortType.MODEL)})
@NameCn("cross候选特征选择训练")
@NameEn("Cross Candidate Selector Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CrossCandidateSelector")
public class CrossCandidateSelectorTrainLocalOp
	extends BaseCrossTrainLocalOp <CrossCandidateSelectorTrainLocalOp>
	implements CrossCandidateSelectorTrainParams <CrossCandidateSelectorTrainLocalOp> {

	public CrossCandidateSelectorTrainLocalOp() {
		this(new Params());
	}

	public CrossCandidateSelectorTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	List <Row> buildAcModelData(List <Tuple3 <Double, Double, Vector>> trainData,
								int[] featureSize,
								DataColumnsSaver dataColumnsSaver) {
		String[] numericalCols = dataColumnsSaver.numericalCols;
		String[] categoricalCols = dataColumnsSaver.categoricalCols;
		int[] numericalIndices = dataColumnsSaver.numericalIndices;

		List <Tuple4 <int[], Double, double[], Integer>> canAndAuc
			= calcAuc(trainData, featureSize, categoricalCols, numericalIndices.length, getFeatureCandidates());

		FeatureSet featureSet = new FeatureSet(featureSize);
		featureSet.numericalCols = numericalCols;
		featureSet.indexSize = featureSize;
		featureSet.vecColName = oneHotVectorCol;
		featureSet.hasDiscrete = true;
		canAndAuc.sort(new Comparator <Tuple4 <int[], Double, double[], Integer>>() {
			@Override
			public int compare(Tuple4 <int[], Double, double[], Integer> o1,
							   Tuple4 <int[], Double, double[], Integer> o2) {
				return -o1.f1.compareTo(o2.f1);
			}
		});

		List <Row> acModel = new ArrayList <>();
		for (int i = 0; i < getCrossFeatureNumber(); i++) {
			featureSet.addOneCrossFeature(canAndAuc.get(i).f0, canAndAuc.get(i).f1);
		}
		acModel.add(Row.of(0L, featureSet.toString(), null));
		for (int i = 0; i < featureSet.crossFeatureSet.size(); i++) {
			acModel.add(Row.of((long) (i + 1), JsonConverter.toJson(featureSet.crossFeatureSet.get(i)),
				featureSet.scores.get(i)));
		}

		return acModel;
	}

	@Override
	void buildSideOutput(OneHotTrainLocalOp oneHotModel, List <Row> acModel, List <String> numericalCols) {

	}

	private static List <Tuple4 <int[], Double, double[], Integer>> calcAuc(
		List <Tuple3 <Double, Double, Vector>> trainData, int[] featureSize,
		String[] inputCols, int numericalSize, String[] featureCandidates) {
		List <int[]> candidateIndices = new ArrayList <>(featureCandidates.length);
		for (String stringCandidate : featureCandidates) {
			String[] candidate = stringCandidate.split(",");
			for (int i = 0; i < candidate.length; i++) {
				candidate[i] = candidate[i].trim();
			}
			candidateIndices.add(TableUtil.findColIndices(inputCols, candidate));
		}

		FeatureEvaluator evaluator = new FeatureEvaluator(
			LinearModelType.LR,
			trainData,
			featureSize,
			null,
			0.8,
			false,
			1);

		List <Tuple4 <int[], Double, double[], Integer>> out = new ArrayList <>();
		for (int i = 0; i < candidateIndices.size(); i++) {
			int[] candidate = candidateIndices.get(i);
			List <int[]> testingFeatures = new ArrayList <>();
			testingFeatures.add(candidate);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("evaluating " + JsonConverter.toJson(testingFeatures));
			}
			Tuple2 <Double, double[]> scoreCoef = evaluator.score(testingFeatures, numericalSize);
			double score = scoreCoef.f0;
			out.add(Tuple4.of(candidate, score, scoreCoef.f1, 0));
		}
		return out;
	}

}
