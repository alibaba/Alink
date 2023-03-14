package com.alibaba.alink.operator.local.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.feature.AutoCross.DataProfile;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureEvaluator;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureSet;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.List;

//todo here we do not support discretization of numerical cols and only keep them unchanged.
//todo if want to discretizate numerical cols, make another op.
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.CROSSED_FEATURES)
})
@NameCn("AutoCross训练")
@NameEn("AutoCross Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.AutoCross")
public class AutoCrossTrainLocalOp extends BaseCrossTrainLocalOp <AutoCrossTrainLocalOp> {

	public AutoCrossTrainLocalOp() {
		this(new Params());
	}

	public AutoCrossTrainLocalOp(Params params) {
		super(params);
	}

	public static final String AC_TRAIN_DATA = "AC_TRAIN_DATA";
	public static final int SESSION_ID = SessionSharedData.getNewSessionId();

	@Override
	List <Row> buildAcModelData(List <Tuple3 <Double, Double, Vector>> trainData,
								int[] featureSize,
								DataColumnsSaver dataColumnsSaver) {
		String[] numericalCols = dataColumnsSaver.numericalCols;
		double fraction = getFraction();
		int kCross = getKCross();
		boolean toFixCoef = true;

		final LinearModelType linearModelType = LinearModelType.LR; // todo: support regression and multi-class cls.

		Tuple2 <List <Tuple3 <Double, Double, Vector>>, List <Tuple3 <Double, Double, Vector>>> splited
			= FeatureEvaluator.split(trainData, fraction, 202303);
		DataProfile profile = new DataProfile(linearModelType, true);
		LinearModelData model = FeatureEvaluator.train(splited.f0, profile);
		DenseVector dv = model.coefVector;
		double score = FeatureEvaluator.evaluate(model, splited.f1);

		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("origin score: " + score);
		}
		Tuple2 <double[], Double> initialCoefsAndScore = Tuple2.of(dv.getData(), score);

		FeatureSet featureSet = new FeatureSet(featureSize);
		featureSet.updateFixedCoefs(initialCoefsAndScore.f0);

		/*
		 * This iteration implements the beam search algorithm as described in
		 * "AutoCross: Automatic Feature Crossing for Tabular Data in Real-World Applications".
		 */
		final int maxSearchStep = getMaxSearchStep();

		final int numThreads = LocalOptimizer.getNumThreads(trainData, getParams());

		for (int step = 0; step < maxSearchStep; step++) {
			TaskRunner taskRunner = new TaskRunner();
			final List <int[]> candidates = featureSet.generateCandidateCrossFeatures();
			final int numCandidates = candidates.size();
			Tuple3 <int[], Double, double[]>[] crossFeatureAndScore = new Tuple3[numCandidates];
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						double[] fixedCoefs = featureSet.getFixedCoefs();
						FeatureEvaluator evaluator = new FeatureEvaluator(
							linearModelType,
							trainData,
							featureSize,
							fixedCoefs,
							fraction,
							toFixCoef,
							kCross);
						for (int kk = curThread; kk < numCandidates; kk += numThreads) {
							int[] candidate = candidates.get(kk);
							List <int[]> testingFeatures = new ArrayList <>(featureSet.crossFeatureSet);
							testingFeatures.add(candidate);
							if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
								System.out.println("curThread:" + curThread + " evaluating " + JsonConverter.toJson(testingFeatures));
							}
							Tuple2 <Double, double[]> scoreCoef
								= evaluator.score(testingFeatures, numericalCols.length);
							if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
								System.out.println("curThread:" + curThread + ", " + JsonConverter.toJson(testingFeatures) + " evaluating score: " + scoreCoef.f0);
							}
							crossFeatureAndScore[kk] = Tuple3.of(candidate, scoreCoef.f0, scoreCoef.f1);
						}
					}
				);
			}
			taskRunner.join();

			Tuple3 <int[], Double, double[]> theBestOne = crossFeatureAndScore[0];
			for (int i = 1; i < crossFeatureAndScore.length; i++) {
				if (crossFeatureAndScore[i].f1 > theBestOne.f1) {
					theBestOne = crossFeatureAndScore[i];
				}
			}

			if(AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("best auc: " + theBestOne.f1 + JsonConverter.toJson(theBestOne.f0));
			}

			featureSet.addOneCrossFeature(theBestOne.f0, theBestOne.f1);
			featureSet.updateFixedCoefs(theBestOne.f2);
		}

		List <Row> acModel = new ArrayList <>();
		featureSet.numericalCols = numericalCols;
		featureSet.indexSize = featureSize;
		featureSet.vecColName = oneHotVectorCol;
		featureSet.hasDiscrete = true;
		acModel.add(Row.of(0L, featureSet.toString(), null));
		for (int i = 0; i < featureSet.crossFeatureSet.size(); i++) {
			acModel.add(Row.of((long) (i + 1), JsonConverter.toJson(featureSet.crossFeatureSet.get(i)),
				featureSet.scores.get(i)));
		}

		return acModel;
	}

	@Override
	void buildSideOutput(OneHotTrainLocalOp oneHotModel, List <Row> acModel,
						 List <String> numericalCols) {
	}

	//private static class CrossFeatureOperation extends
	//	RichMapPartitionFunction <Tuple3 <Double, Double, Vector>,
	//		Tuple3 <int[], Double, double[]>> {
	//	private static final long serialVersionUID = -4682615150965402842L;
	//	transient FeatureSet featureSet;
	//	transient int numTasks;
	//	transient List <int[]> candidates;
	//	private int[] featureSize;
	//	int numericalSize;
	//	private final LinearModelType linearModelType;
	//	private final double fraction;
	//	private final boolean toFixCoef;
	//	private final int kCross;
	//
	//	CrossFeatureOperation(int numericalSize, LinearModelType linearModelType,
	//						  double fraction, boolean toFixCoef, int kCross) {
	//		this.numericalSize = numericalSize;
	//		this.linearModelType = linearModelType;
	//		this.fraction = fraction;
	//		this.toFixCoef = toFixCoef;
	//		this.kCross = kCross;
	//	}
	//
	//	@Override
	//	public void open(Configuration parameters) throws Exception {
	//		numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
	//		featureSet = (FeatureSet) getRuntimeContext().getBroadcastVariable("featureSet").get(0);
	//		featureSize = (int[]) getIterationRuntimeContext().getBroadcastVariable("featureSize").get(0);
	//		//generate candidate feature combination.
	//		candidates = featureSet.generateCandidateCrossFeatures();
	//		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
	//			System.out.println(String.format("\n** step %d, # picked features %d, # total candidates %d",
	//				getIterationRuntimeContext().getSuperstepNumber(), featureSet.crossFeatureSet.size(),
	//				candidates.size()));
	//		}
	//	}
	//
	//	@Override
	//	public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> values,
	//							 Collector <Tuple3 <int[], Double, double[]>> out) throws
	//		Exception {
	//		int taskId = getRuntimeContext().getIndexOfThisSubtask();
	//		if (candidates.size() <= taskId) {
	//			return;
	//		}
	//
	//		List <Tuple3 <Double, Double, Vector>> data = (List <Tuple3 <Double, Double, Vector>>)
	//			SessionSharedData.get(AC_TRAIN_DATA, SESSION_ID, taskId);
	//		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
	//			System.out.println("taskId: " + taskId + ", data size: " + data.size());
	//		}
	//		double[] fixedCoefs = featureSet.getFixedCoefs();
	//		FeatureEvaluator evaluator = new FeatureEvaluator(
	//			linearModelType,
	//			data,
	//			featureSize,
	//			fixedCoefs,
	//			fraction,
	//			toFixCoef,
	//			kCross);
	//		//distribution candidates to each partition.
	//		for (int i = taskId; i < candidates.size(); i += numTasks) {
	//			int[] candidate = candidates.get(i);
	//			List <int[]> testingFeatures = new ArrayList <>(featureSet.crossFeatureSet);
	//			testingFeatures.add(candidate);
	//			//testingFeatures.add(new int[] {0, 1, 2});
	//			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
	//				System.out.println("evaluating " + JsonConverter.toJson(testingFeatures));
	//			}
	//			Tuple2 <Double, double[]> scoreCoef = evaluator.score(testingFeatures, numericalSize);
	//			double score = scoreCoef.f0;
	//			out.collect(Tuple3.of(candidate, score, scoreCoef.f1));
	//		}
	//	}
	//}

	//private static class BuildModel extends RichFlatMapFunction <FeatureSet, Row> {
	//	private static final long serialVersionUID = -3939236593612638919L;
	//	private int[] indexSize;
	//	private String vectorCol;
	//	private boolean hasDiscrete;
	//	private String[] numericalCols;
	//
	//	BuildModel(String vectorCol, String[] numericalCols, boolean hasDiscrete) {
	//		this.vectorCol = vectorCol;
	//		this.hasDiscrete = hasDiscrete;
	//		this.numericalCols = numericalCols;
	//	}
	//
	//	@Override
	//	public void open(Configuration parameters) throws Exception {
	//		super.open(parameters);
	//		indexSize = (int[]) getRuntimeContext().getBroadcastVariable("featureSize").get(0);
	//	}
	//
	//	@Override
	//	public void flatMap(FeatureSet fs, Collector <Row> out) throws Exception {
	//		fs.numericalCols = numericalCols;
	//		fs.indexSize = indexSize;
	//		fs.vecColName = vectorCol;
	//		fs.hasDiscrete = hasDiscrete;
	//		out.collect(Row.of(0L, fs.toString(), null));
	//		for (int i = 0; i < fs.crossFeatureSet.size(); i++) {
	//			out.collect(Row.of((long) (i + 1), JsonConverter.toJson(fs.crossFeatureSet.get(i)), fs.scores.get(i)));
	//		}
	//	}
	//}
}
