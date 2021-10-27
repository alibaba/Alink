package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import com.alibaba.alink.pipeline.classification.Softmax;

import java.io.File;

public class Chap12 {

	static final String DATA_DIR = Utils.ROOT_DIR + "iris" + File.separator;

	static final String ORIGIN_FILE = "iris.data";

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";

	static final String SCHEMA_STRING
		= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	static final String[] FEATURE_COL_NAMES
		= new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width"};

	static final String LABEL_COL_NAME = "category";

	static final String PREDICTION_COL_NAME = "pred";
	static final String PRED_DETAIL_COL_NAME = "pred_info";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_2();

		c_3();

		c_4();

		c_5();

		c_6();

	}

	static void c_2() throws Exception {
		CsvSourceBatchOp source =
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING);

		source
			.lazyPrint(5, "origin file")
			.lazyPrintStatistics("stat of origin file")
			.link(
				new CorrelationBatchOp()
					.setSelectedCols(FEATURE_COL_NAMES)
					.lazyPrintCorrelation()
			);

		source.groupBy(LABEL_COL_NAME, LABEL_COL_NAME + ", COUNT(*) AS cnt").lazyPrint(-1);

		BatchOperator.execute();

		Utils.splitTrainTestIfNotExist(source, DATA_DIR + TRAIN_FILE, DATA_DIR + TEST_FILE, 0.9);

	}

	static void c_3() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		NaiveBayesTrainBatchOp trainer =
			new NaiveBayesTrainBatchOp()
				.setFeatureCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME);

		NaiveBayesPredictBatchOp predictor =
			new NaiveBayesPredictBatchOp()
				.setPredictionCol(PREDICTION_COL_NAME)
				.setPredictionDetailCol(PRED_DETAIL_COL_NAME);

		train_data.link(trainer);

		predictor.linkFrom(trainer, test_data);

		trainer.lazyPrintModelInfo();

		predictor.lazyPrint(1, "< Prediction >");

		predictor
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.lazyPrintMetrics("NaiveBayes")
			);

		BatchOperator.execute();

	}

	static void c_4() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new OneVsRest()
			.setClassifier(
				new LogisticRegression()
					.setFeatureCols(FEATURE_COL_NAMES)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.setNumClass(3)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("OneVsRest_LogisticRegression")
			);

		new OneVsRest()
			.setClassifier(
				new GbdtClassifier()
					.setFeatureCols(FEATURE_COL_NAMES)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.setNumClass(3)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("OneVsRest_GBDT")
			);

		new OneVsRest()
			.setClassifier(
				new LinearSvm()
					.setFeatureCols(FEATURE_COL_NAMES)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.setNumClass(3)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("OneVsRest_LinearSvm")
			);

		BatchOperator.execute();

	}

	static void c_5() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new Softmax()
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintTrainInfo()
			.enableLazyPrintModelInfo()
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("Softmax")
			);

		BatchOperator.execute();

	}

	static void c_6() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new MultilayerPerceptronClassifier()
			.setLayers(new int[] {4, 12, 3})
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("MultilayerPerceptronClassifier [4, 12, 3]")
			);

		new MultilayerPerceptronClassifier()
			.setLayers(new int[] {4, 3})
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("MultilayerPerceptronClassifier [4, 3]")
			);

		BatchOperator.execute();

	}

}