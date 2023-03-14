package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

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
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.feature.AutoCross.BuildSideOutput;
import com.alibaba.alink.operator.common.feature.AutoCross.DataProfile;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureEvaluator;
import com.alibaba.alink.operator.common.feature.AutoCross.FeatureSet;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
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
public class AutoCrossTrainBatchOp extends BaseCrossTrainBatchOp <AutoCrossTrainBatchOp> {

	private static final long serialVersionUID = 2847616118502942858L;

	public AutoCrossTrainBatchOp() {
		this(new Params());
	}

	public AutoCrossTrainBatchOp(Params params) {
		super(params);
	}
	public static final String AC_TRAIN_DATA = "AC_TRAIN_DATA";
	public static final int SESSION_ID = SessionSharedData.getNewSessionId();

	DataSet <Row> buildAcModelData(DataSet<Tuple3 <Double, Double, Vector>> trainData,
								   DataSet <int[]> featureSizeDataSet,
								   DataColumnsSaver dataColumnsSaver) {
		String[] numericalCols = dataColumnsSaver.numericalCols;
		double fraction = getFraction();
		int kCross = getKCross();
		boolean toFixCoef = true;

		final LinearModelType linearModelType = LinearModelType.LR; // todo: support regression and multi-class cls.

		DataSet <Tuple2<double[], Double>> initialCoefsAndScore = trainData
			.mapPartition(
				new RichMapPartitionFunction <Tuple3 <Double, Double, Vector>, Tuple2<double[], Double>>() {
					@Override
					public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> values,
											 Collector <Tuple2<double[], Double>> out) throws Exception {
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						if (taskId != 0) {
							return;
						}
						List <Tuple3 <Double, Double, Vector>> samples =
							(List <Tuple3 <Double, Double, Vector>>) SessionSharedData.get(AC_TRAIN_DATA, SESSION_ID, taskId);

						Tuple2 <List <Tuple3 <Double, Double, Vector>>, List <Tuple3 <Double, Double, Vector>>>
							splited =
							FeatureEvaluator.split(samples, fraction, 0);
						DataProfile profile = new DataProfile(linearModelType, true);
						LinearModelData model = FeatureEvaluator.train(splited.f0, profile);
						DenseVector dv = model.coefVector;
						double score = FeatureEvaluator.evaluate(model, splited.f1);
						out.collect(Tuple2.of(dv.getData(), score));
					}
				}).withBroadcastSet(trainData, "barrier");

		DataSet <FeatureSet> featureSet = featureSizeDataSet
			.map(new RichMapFunction <int[], FeatureSet>() {
				Tuple2<double[], Double> initialCoef;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					this.initialCoef = (Tuple2<double[], Double>) getRuntimeContext().getBroadcastVariable("initialCoefsAndScore").get(0);
				}

				private static final long serialVersionUID = 2539121870837769846L;

				@Override
				public FeatureSet map(int[] value) throws Exception {
					//the value is all the categorical cols.
					FeatureSet featureSet = new FeatureSet(value);
					featureSet.updateFixedCoefs(initialCoef.f0);
					return featureSet;
				}
			}).withBroadcastSet(initialCoefsAndScore, "initialCoefsAndScore");


		/*
		 * This iteration implements the beam search algorithm as described in
		 * "AutoCross: Automatic Feature Crossing for Tabular Data in Real-World Applications".
		 */
		final int maxSearchStep = getMaxSearchStep();
		IterativeDataSet <FeatureSet> loop = featureSet.iterate(maxSearchStep);

		DataSet <Tuple3 <int[], Double, double[]>> crossFeatureAndScore = trainData
			.mapPartition(new CrossFeatureOperation(numericalCols.length, linearModelType, fraction, toFixCoef, kCross))
			.withBroadcastSet(loop, "featureSet")
			.withBroadcastSet(featureSizeDataSet, "featureSize")
			.withBroadcastSet(trainData, "barrier")
			.name("train_and_evaluate");

		//return candidate indices, score and fixed coefs.
		DataSet <Tuple3 <int[], Double, double[]>> theBestOne = crossFeatureAndScore
			.reduce(new ReduceFunction <Tuple3 <int[], Double, double[]>>() {
				private static final long serialVersionUID = 1099754368531239834L;

				@Override
				public Tuple3 <int[], Double, double[]> reduce(Tuple3 <int[], Double, double[]> value1,
															   Tuple3 <int[], Double, double[]> value2)
					throws Exception {
					return value1.f1 > value2.f1 ? value1 : value2;
				}
			}).name("reduce the best one");

		DataSet <FeatureSet> updatedFeatureSet = loop
			.map(new RichMapFunction <FeatureSet, FeatureSet>() {

				private static final long serialVersionUID = 1017420195682258788L;

				@Override
				public FeatureSet map(FeatureSet fs) {
					List <Tuple3 <int[], Double, double[]>> bc = getRuntimeContext()
						.getBroadcastVariable("the_one");

					if (bc.size() == 0) {//todo check can its size be 0?
						return fs;
					}
					fs.addOneCrossFeature(bc.get(0).f0, bc.get(0).f1);
					fs.updateFixedCoefs(bc.get(0).f2);
					return fs;
				}
			})
			.withBroadcastSet(theBestOne, "the_one")
			.name("update feature set");

		featureSet = loop.closeWith(updatedFeatureSet, theBestOne);

		DataSet <Row> acModel = featureSet
			.flatMap(new BuildModel(oneHotVectorCol, numericalCols, hasDiscrete))
			.withBroadcastSet(featureSizeDataSet, "featureSize");
		return acModel;
	}

	void buildSideOutput(OneHotTrainBatchOp oneHotModel, DataSet <Row> acModel,
						 List <String> numericalCols, long mlEnvId) {
		DataSet <Row> sideDataSet = oneHotModel.getDataSet()
			.mapPartition(new BuildSideOutput(numericalCols.size()))
			.withBroadcastSet(acModel, "autocrossModel")
			.setParallelism(1);

		Table sideModel = DataSetConversionUtil.toTable(mlEnvId, sideDataSet,
			new String[] {"index", "feature", "value"},
			new TypeInformation[] {Types.INT, Types.STRING, Types.STRING});
		this.setSideOutputTables(new Table[] {sideModel});
	}


	private static class CrossFeatureOperation extends
		RichMapPartitionFunction <Tuple3 <Double, Double, Vector>,
			Tuple3 <int[], Double, double[]>> {
		private static final long serialVersionUID = -4682615150965402842L;
		transient FeatureSet featureSet;
		transient int numTasks;
		transient List <int[]> candidates;
		private int[] featureSize;
		int numericalSize;
		private final LinearModelType linearModelType;
		private final double fraction;
		private final boolean toFixCoef;
		private final int kCross;

		CrossFeatureOperation(int numericalSize, LinearModelType linearModelType,
							  double fraction, boolean toFixCoef, int kCross) {
			this.numericalSize = numericalSize;
			this.linearModelType = linearModelType;
			this.fraction = fraction;
			this.toFixCoef = toFixCoef;
			this.kCross = kCross;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			featureSet = (FeatureSet) getRuntimeContext().getBroadcastVariable("featureSet").get(0);
			featureSize = (int[]) getIterationRuntimeContext().getBroadcastVariable("featureSize").get(0);
			//generate candidate feature combination.
			candidates = featureSet.generateCandidateCrossFeatures();
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("\n** step %d, # picked features %d, # total candidates %d",
					getIterationRuntimeContext().getSuperstepNumber(), featureSet.crossFeatureSet.size(),
					candidates.size()));
			}
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Double, Vector>> values,
								 Collector <Tuple3 <int[], Double, double[]>> out) throws
			Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (candidates.size() <= taskId) {
				return;
			}

			List <Tuple3 <Double, Double, Vector>> data = (List <Tuple3 <Double, Double, Vector>>)
				SessionSharedData.get(AC_TRAIN_DATA, SESSION_ID, taskId);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("taskId: " + taskId + ", data size: " + data.size());
			}
			double[] fixedCoefs = featureSet.getFixedCoefs();
			FeatureEvaluator evaluator = new FeatureEvaluator(
				linearModelType,
				data,
				featureSize,
				fixedCoefs,
				fraction,
				toFixCoef,
				kCross);
			//distribution candidates to each partition.
			for (int i = taskId; i < candidates.size(); i += numTasks) {
				int[] candidate = candidates.get(i);
				List <int[]> testingFeatures = new ArrayList <>(featureSet.crossFeatureSet);
				testingFeatures.add(candidate);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("evaluating " + JsonConverter.toJson(testingFeatures));
				}
				Tuple2 <Double, double[]> scoreCoef = evaluator.score(testingFeatures, numericalSize);
				double score = scoreCoef.f0;
				out.collect(Tuple3.of(candidate, score, scoreCoef.f1));
			}
		}
	}

	private static class BuildModel extends RichFlatMapFunction <FeatureSet, Row> {
		private static final long serialVersionUID = -3939236593612638919L;
		private int[] indexSize;
		private String vectorCol;
		private boolean hasDiscrete;
		private String[] numericalCols;

		BuildModel(String vectorCol, String[] numericalCols, boolean hasDiscrete) {
			this.vectorCol = vectorCol;
			this.hasDiscrete = hasDiscrete;
			this.numericalCols = numericalCols;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			indexSize = (int[]) getRuntimeContext().getBroadcastVariable("featureSize").get(0);
		}

		@Override
		public void flatMap(FeatureSet fs, Collector <Row> out) throws Exception {
			fs.numericalCols = numericalCols;
			fs.indexSize = indexSize;
			fs.vecColName = vectorCol;
			fs.hasDiscrete = hasDiscrete;
			out.collect(Row.of(0L, fs.toString(), null));
			for (int i = 0; i < fs.crossFeatureSet.size(); i++) {
				out.collect(Row.of((long) (i + 1), JsonConverter.toJson(fs.crossFeatureSet.get(i)), fs.scores.get(i)));
			}
		}
	}
}
