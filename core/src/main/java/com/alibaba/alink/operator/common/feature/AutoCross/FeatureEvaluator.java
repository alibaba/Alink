package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.local.classification.BaseLinearModelTrainLocalOp;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class FeatureEvaluator {
	private static final boolean HAS_INTERCEPT = true;
	private LinearModelType linearModelType;
	private final List <Tuple3 <Double, Double, Vector>> data;
	private double[] fixedCoefs;
	private int[] featureSize;
	private double fraction;
	private boolean toFixCoef;
	private int kCross;

	public FeatureEvaluator(LinearModelType linearModelType,
							List <Tuple3 <Double, Double, Vector>> data,
							int[] featureSize,
							double[] fixedCoefs,
							double fraction,
							boolean toFixCoef,//这边是从外面传进来的，少不了。
							int kCross) {
		this.linearModelType = linearModelType;
		this.data = data;
		this.featureSize = featureSize;
		this.fixedCoefs = fixedCoefs;
		this.fraction = fraction;
		this.toFixCoef = toFixCoef;
		this.kCross = kCross;
	}

	//calculate one group of features with all data.
	public Tuple2 <Double, double[]> score(List <int[]> crossFeatures, int numericalSize) {
		DataProfile profile = new DataProfile(linearModelType, HAS_INTERCEPT);
		List <Tuple3 <Double, Double, Vector>> dataWithCrossFea = expandFeatures(data, crossFeatures, featureSize,
			numericalSize);
		System.out.println(JsonConverter.toJson(crossFeatures) + ",vector size: " + dataWithCrossFea.get(0).f2.size());
		LinearModelData model = null;
		double score = 0.;
		for (int i = 0; i < kCross; i++) {
			Tuple2 <List <Tuple3 <Double, Double, Vector>>, List <Tuple3 <Double, Double, Vector>>> splited =
				split(dataWithCrossFea, fraction, i);// i is the random seed.
			//train the model with half data and evaluation with another half.
			model = train(splited.f0, profile, toFixCoef, fixedCoefs);
			score += evaluate(model, splited.f1);//return the auc of predict data.
		}

		return Tuple2.of(score / kCross, model.coefVector.getData());
	}

	public static LinearModelData train(List <Tuple3 <Double, Double, Vector>> samples, DataProfile profile,
										boolean toFixCoef, double[] fixedCoefs) {
		return train(samples, profile);
	}

	public static LinearModelData train(List <Tuple3 <Double, Double, Vector>> samples, DataProfile profile) {
		final LinearModelType linearModelType = LinearModelType.LR;

		final OptimObjFunc objFunc = OptimObjFunc.getObjFunction(linearModelType, new Params());
		final boolean remapLabelValue = linearModelType.equals(LinearModelType.LR);

		samples = samples.stream()
			.map(sample -> {
				Vector features = profile.hasIntercept ? sample.f2.prefix(1.0) : sample.f2;
				double label = sample.f1;
				if (remapLabelValue) {
					label = label == 0. ? 1.0 : -1.0;
				}
				return Tuple3.of(sample.f0, label, features);
			})
			.collect(Collectors.toList());

		Params optParams = new Params()
			//.set(HasNumThreads.NUM_THREADS, 1)
			.set(LinearTrainParams.WITH_INTERCEPT, profile.hasIntercept)
			.set(LinearTrainParams.STANDARDIZATION, false);


		int weightSize = samples.get(0).f2.size();//intercept has been added before.
		double[] iniWeight = new double[weightSize];
		Arrays.fill(iniWeight, 1e-4);
		DenseVector initialWeights = new DenseVector(iniWeight);

		Tuple2 <DenseVector, double[]> weightsAndLoss = LocalOptimizer.optimize(objFunc, samples, initialWeights, optParams);

		Params meta = new Params();
		Double[] labelValues = new Double[profile.numDistinctLabels];
		for (int i = 0; i < labelValues.length; i++) {
			labelValues[i] = (double) i;
		}
		meta.set(ModelParamName.MODEL_NAME, "model");
		meta.set(ModelParamName.LINEAR_MODEL_TYPE, linearModelType);
		meta.set(ModelParamName.LABEL_VALUES, labelValues);
		meta.set(ModelParamName.HAS_INTERCEPT_ITEM, profile.hasIntercept);
		meta.set(ModelParamName.VECTOR_COL_NAME, "features");
		//those params are set just for build linear model data.
		meta.set(LinearTrainParams.LABEL_COL, null);
		meta.set(ModelParamName.FEATURE_TYPES, null);

		return BaseLinearModelTrainLocalOp.buildLinearModelData(meta,
			null,
			Types.DOUBLE,
			null,
			profile.hasIntercept,
			false,
			weightsAndLoss
		);
	}

	public static double evaluate(LinearModelData model, List <Tuple3 <Double, Double, Vector>> samples) {

		LinearModelMapper modelMapper = new LinearModelMapper(
			new LinearModelDataConverter(Types.DOUBLE).getModelSchema(),
			new TableSchema(new String[] {"features", "label"}, new TypeInformation[] {Types.STRING, Types.DOUBLE}),
			new Params()
				.set(LinearModelMapperParams.VECTOR_COL, "features")
				.set(LinearModelMapperParams.PREDICTION_COL, "prediction_result")
				.set(LinearModelMapperParams.PREDICTION_DETAIL_COL, "prediction_detail")
				.set(LinearModelMapperParams.RESERVED_COLS, new String[] {"label"})
		);
		modelMapper.loadModel(model);

		List <Row> predictions = samples.stream()
			.map(t3 -> {
				Row row = Row.of(t3.f2, t3.f1);
				try {
					Row pred = modelMapper.map(row);
					return Row.of(pred.getField(0), pred.getField(2));
				} catch (Exception e) {
					throw new RuntimeException("Fail to predict.", e);
				}
			})
			.collect(Collectors.toList());

		if (model.linearModelType.equals(LinearModelType.LR)) {
			BinaryMetricsSummary metricsSummary = (BinaryMetricsSummary) EvaluationUtil.getDetailStatistics
				(predictions,
					"1.0", true, Types.DOUBLE);
			BinaryClassMetrics metrics = metricsSummary.toMetrics();
			return metrics.getAuc();
		} else {
			throw new UnsupportedOperationException("Not yet supported model type: " + model.linearModelType);
		}
	}

	//split all the samples to two parts.
	public static Tuple2 <List <Tuple3 <Double, Double, Vector>>, List <Tuple3 <Double, Double, Vector>>>
	split(List <Tuple3 <Double, Double, Vector>> samples,
		  double fraction,
		  int seed) {
		List <Tuple3 <Double, Double, Vector>> part1 = new ArrayList <>(samples.size() / 2 + 1);
		List <Tuple3 <Double, Double, Vector>> part2 = new ArrayList <>(samples.size() / 2 + 1);
		int dataSize = samples.size();
		if (dataSize < 2) {
			throw new RuntimeException("Data size is too small!");
		}
		part1.add(samples.get(0));
		part2.add(samples.get(1));
		Random rand = new Random(seed);
		for (int i = 2; i < samples.size(); i++) {
			if (rand.nextDouble() <= fraction) {
				part1.add(samples.get(i));
			} else {
				part2.add(samples.get(i));
			}
		}
		return Tuple2.of(part1, part2);
	}

	/**
	 * Generate data for training and evaluation by onehoting the raw features and crossed features.
	 */
	public static List <Tuple3 <Double, Double, Vector>> expandFeatures(List <Tuple3 <Double, Double, Vector>> data,
																		List <int[]> crossFeatures,
																		int[] featureSize, int numericalSize) {
		int featureNum = featureSize.length;//输入了这么多个categorical的特征。
		int originVectorSize = data.get(0).f2.size();
		int crossFeatureSize = crossFeatures.size();
		int[] cumsumFeatureSize = new int[featureSize.length];//累积加起来的，在dot的时候用到。
		for (int i = 0; i < featureSize.length; i++) {
			if (i == 0) {
				cumsumFeatureSize[i] = 0;
			} else {
				cumsumFeatureSize[i] = cumsumFeatureSize[i - 1] + featureSize[i - 1];
			}
		}
		int[][] carry = new int[crossFeatureSize][];//onehot进制
		for (int i = 0; i < crossFeatureSize; i++) {
			int[] candidateFeature = crossFeatures.get(i);
			for (int j = 0; j < candidateFeature.length; j++) {
				if (j == 0) {
					carry[i] = new int[candidateFeature.length];
					carry[i][j] = 1;
				} else {
					carry[i][j] = carry[i][j - 1] * featureSize[candidateFeature[j - 1]];
				}
			}
		}
		List <Tuple3 <Double, Double, Vector>> resData = new ArrayList <>(data.size());

		//我需要知道每个candidate有多少个取值。然后每个都拼起来。
		//现在这种做法可以解决之前的问题。也就是训练的时候过于稀疏。
		//第一个存的是每个candidate的可选，后第二个存的是每条数据在每个candidate上的index。
		int[] candidateNumber = new int[crossFeatureSize];
		int[][] featureIndices = new int[data.size()][crossFeatureSize];
		for (int i = 0; i < crossFeatureSize; i++) {
			//Map <Integer, Integer> tokenToInt = new HashMap <>();
			//int count = 0;
			for (int j = 0; j < data.size(); j++) {
				int[] originIndices = ((SparseVector) data.get(j).f2).getIndices().clone();
				for (int k = numericalSize; k < originIndices.length; k++) {
					originIndices[k] -= numericalSize;
				}
				//index是组合在当前cross里的位置
				int index = dot(carry[i], crossFeatures.get(i), originIndices, numericalSize, cumsumFeatureSize);
				if (index < 0) {
					System.out.println();
				}
				//if (!tokenToInt.containsKey(index)) {
				//	tokenToInt.put(index, count++);
				//}
				featureIndices[j][i] = index;
				//featureIndices[j][i] = tokenToInt.get(index);//记不清干嘛的了？
			}
			int count = 1;
			for (int featureIndex : crossFeatures.get(i)) {
				count *= featureSize[featureIndex];
			}
			candidateNumber[i] = count;//这个很明确，每个candidate的数目。
		}

		//candidateCrossIndex是每组cross feature开始的index
		int[] candidateCrossIndex = new int[crossFeatureSize];
		candidateCrossIndex[0] = originVectorSize;
		for (int i = 1; i < crossFeatureSize; i++) {
			candidateCrossIndex[i] = candidateCrossIndex[i - 1] + candidateNumber[i - 1];
		}

		//featureIndices是每个交叉特征的位置
		for (int i = 0; i < data.size(); i++) {
			for (int j = 0; j < crossFeatureSize; j++) {
				featureIndices[i][j] += candidateCrossIndex[j];
			}
		}

		//最后一组cross特征的起始位置
		int resVecSize = candidateCrossIndex[crossFeatureSize - 1] + candidateNumber[crossFeatureSize - 1];
		//非零元的indices个数
		int newVecElementSize = numericalSize + featureNum + crossFeatureSize;
		for (int i = 0; i < data.size(); i++) {
			Tuple3 <Double, Double, Vector> datum = data.get(i);
			int[] originIndices = ((SparseVector) datum.f2).getIndices();
			int[] newIndices = new int[newVecElementSize];
			System.arraycopy(originIndices, 0, newIndices, 0, featureNum + numericalSize);
			System.arraycopy(featureIndices[i], 0, newIndices, featureNum + numericalSize, crossFeatureSize);
			double[] newValues = new double[newVecElementSize];
			Arrays.fill(newValues, 1.0);
			System.arraycopy(((SparseVector) datum.f2).getValues(), 0, newValues, 0, numericalSize);
			//todo 存fixed。
			resData.add(Tuple3.of(datum.f0, datum.f1,
				new SparseVector(resVecSize, newIndices, newValues)));
		}

		//
		//        for (Tuple3<Double, Double, Vector> datum : data) {
		//            int[] originIndices = ((SparseVector) datum.f2).getIndices();
		//            int indicesSize = originIndices.length + crossFeatureSize;
		//            int[] newIndices = new int[indicesSize];
		//            System.arraycopy(originIndices, 0, newIndices, 0, originIndices.length);
		//            for (int i = 0; i < crossFeatureSize; i++) {
		//                newIndices[i + originIndices.length] = cunsumCrossFeatureSize[i]
		//                    + dot(carry[i], crossFeatures.get(i), originIndices, cunsumFeatureSize);
		//            }
		//            double[] newValues = new double[indicesSize];
		//            Arrays.fill(newValues, 1.0);
		//            resData.add(Tuple3.of(datum.f0, datum.f1,
		//                new SparseVector(dataSize, newIndices, newValues)));
		//
		//        }
		return resData;
	}

	/*
	 * calculate and consider the carry.
	 * if carry is 1, 3, 9; if the input is 1, 2, 0, then it is the 7th one; the input 0, 1, 0 is the 3rd one.
	 * //considering its real index in vector, we need to minus 1 by the former element count.
	 */
	private static int dot(int[] carry, int[] crossFeatures, int[] originIndices, int numericalSize,
						   int[] cumsumFeatureSize) {
		int res = 0;
		for (int i = 0; i < carry.length; i++) {
			res += carry[i] * (originIndices[crossFeatures[i] + numericalSize] - cumsumFeatureSize[crossFeatures[i]]);
		}
		return res;
	}

}
