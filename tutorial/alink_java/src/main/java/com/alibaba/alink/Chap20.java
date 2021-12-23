package com.alibaba.alink;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.tuning.BinaryClassificationTuningEvaluator;
import com.alibaba.alink.pipeline.tuning.ClusterTuningEvaluator;
import com.alibaba.alink.pipeline.tuning.GridSearchCV;
import com.alibaba.alink.pipeline.tuning.GridSearchCVModel;
import com.alibaba.alink.pipeline.tuning.ParamDist;
import com.alibaba.alink.pipeline.tuning.ParamGrid;
import com.alibaba.alink.pipeline.tuning.RandomSearchTVSplit;
import com.alibaba.alink.pipeline.tuning.RandomSearchTVSplitModel;
import com.alibaba.alink.pipeline.tuning.ValueDist;
import org.apache.commons.lang3.ArrayUtils;

public class Chap20 {

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_1();

		c_2();

		c_3();

	}

	static void c_1() throws Exception {
		BatchOperator <?> train_data =
			new AkSourceBatchOp()
				.setFilePath(Chap10.DATA_DIR + Chap10.TRAIN_FILE)
				.select(Chap10.CLAUSE_CREATE_FEATURES);

		BatchOperator <?> test_data =
			new AkSourceBatchOp()
				.setFilePath(Chap10.DATA_DIR + Chap10.TEST_FILE)
				.select(Chap10.CLAUSE_CREATE_FEATURES);

		final String[] new_features =
			ArrayUtils.removeElement(train_data.getColNames(), Chap10.LABEL_COL_NAME);

		LogisticRegression lr = new LogisticRegression()
			.setFeatureCols(new_features)
			.setLabelCol(Chap10.LABEL_COL_NAME)
			.setPredictionCol(Chap10.PREDICTION_COL_NAME)
			.setPredictionDetailCol(Chap10.PRED_DETAIL_COL_NAME);

		Pipeline pipeline = new Pipeline().add(lr);

		GridSearchCV gridSearch = new GridSearchCV()
			.setNumFolds(5)
			.setEstimator(pipeline)
			.setParamGrid(
				new ParamGrid()
					.addGrid(lr, LogisticRegression.L_1,
						new Double[] {0.0000001, 0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0, 10.0})
			)
			.setTuningEvaluator(
				new BinaryClassificationTuningEvaluator()
					.setLabelCol(Chap10.LABEL_COL_NAME)
					.setPredictionDetailCol(Chap10.PRED_DETAIL_COL_NAME)
					.setTuningBinaryClassMetric(TuningBinaryClassMetric.AUC)
			)
			.enableLazyPrintTrainInfo();

		GridSearchCVModel bestModel = gridSearch.fit(train_data);

		bestModel.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("2")
					.setLabelCol(Chap10.LABEL_COL_NAME)
					.setPredictionDetailCol(Chap10.PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("GridSearchCV")
			);

		BatchOperator.execute();

	}

	static void c_2() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		BatchOperator train_sample =
			new AkSourceBatchOp()
				.setFilePath(Chap11.DATA_DIR + Chap11.TRAIN_SAMPLE_FILE);

		BatchOperator test_data =
			new AkSourceBatchOp()
				.setFilePath(Chap11.DATA_DIR + Chap11.TEST_FILE);

		final String[] featuresColNames =
			ArrayUtils.removeElement(train_sample.getColNames(), Chap11.LABEL_COL_NAME);

		GbdtClassifier gbdt = new GbdtClassifier()
			.setFeatureCols(featuresColNames)
			.setLabelCol(Chap11.LABEL_COL_NAME)
			.setPredictionCol(Chap11.PREDICTION_COL_NAME)
			.setPredictionDetailCol(Chap11.PRED_DETAIL_COL_NAME);

		RandomSearchTVSplit randomSearch = new RandomSearchTVSplit()
			.setNumIter(20)
			.setTrainRatio(0.8)
			.setEstimator(gbdt)
			.setParamDist(
				new ParamDist()
					.addDist(gbdt, GbdtClassifier.NUM_TREES, ValueDist.randArray(new Integer[] {50, 100}))
					.addDist(gbdt, GbdtClassifier.MAX_DEPTH, ValueDist.randInteger(4, 10))
					.addDist(gbdt, GbdtClassifier.MAX_BINS, ValueDist.randArray(new Integer[] {64, 128, 256, 512}))
					.addDist(gbdt, GbdtClassifier.LEARNING_RATE, ValueDist.randArray(new Double[] {0.3, 0.1, 0.01}))
			)
			.setTuningEvaluator(
				new BinaryClassificationTuningEvaluator()
					.setLabelCol(Chap11.LABEL_COL_NAME)
					.setPredictionDetailCol(Chap11.PRED_DETAIL_COL_NAME)
					.setTuningBinaryClassMetric(TuningBinaryClassMetric.F1)
			)
			.enableLazyPrintTrainInfo();

		RandomSearchTVSplitModel bestModel = randomSearch.fit(train_sample);

		bestModel.transform(test_data)
			.link(
				new EvalBinaryClassBatchOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(Chap11.LABEL_COL_NAME)
					.setPredictionDetailCol(Chap11.PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	static void c_3() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		AkSourceBatchOp source =
			new AkSourceBatchOp()
				.setFilePath(Chap17.DATA_DIR + Chap17.VECTOR_FILE);

		KMeans kmeans = new KMeans()
			.setVectorCol(Chap17.VECTOR_COL_NAME)
			.setPredictionCol(Chap17.PREDICTION_COL_NAME);

		GridSearchCV cv = new GridSearchCV()
			.setNumFolds(4)
			.setEstimator(kmeans)
			.setParamGrid(
				new ParamGrid()
					.addGrid(kmeans, KMeans.K, new Integer[] {2, 3, 4, 5, 6})
					.addGrid(kmeans, KMeans.DISTANCE_TYPE,
						new DistanceType[] {DistanceType.EUCLIDEAN, DistanceType.COSINE})
			)
			.setTuningEvaluator(
				new ClusterTuningEvaluator()
					.setVectorCol(Chap17.VECTOR_COL_NAME)
					.setPredictionCol(Chap17.PREDICTION_COL_NAME)
					.setLabelCol(Chap17.LABEL_COL_NAME)
					.setTuningClusterMetric(TuningClusterMetric.RI)
			)
			.enableLazyPrintTrainInfo();

		GridSearchCVModel bestModel = cv.fit(source);

		bestModel
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setLabelCol(Chap17.LABEL_COL_NAME)
					.setVectorCol(Chap17.VECTOR_COL_NAME)
					.setPredictionCol(Chap17.PREDICTION_COL_NAME)
					.lazyPrintMetrics()
			);

		BatchOperator.execute();

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

}
