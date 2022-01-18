package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.CorrelationBatchOp;
import com.alibaba.alink.pipeline.regression.DecisionTreeRegressor;
import com.alibaba.alink.pipeline.regression.GbdtRegressor;
import com.alibaba.alink.pipeline.regression.LassoRegression;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.regression.RandomForestRegressor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;

public class Chap16 {

	static final String DATA_DIR = Utils.ROOT_DIR + "wine" + File.separator;

	private static final String ORIGIN_FILE = "winequality-white.csv";

	static final String TRAIN_FILE = "train.ak";
	static final String TEST_FILE = "test.ak";

	private static final String[] COL_NAMES = new String[] {
		"fixedAcidity", "volatileAcidity", "citricAcid", "residualSugar", "chlorides",
		"freeSulfurDioxide", "totalSulfurDioxide", "density", "pH", "sulphates",
		"alcohol", "quality"
	};

	private static final String[] COL_TYPES = new String[] {
		"double", "double", "double", "double", "double",
		"double", "double", "double", "double", "double",
		"double", "double"
	};

	static final String[] FEATURE_COL_NAMES = ArrayUtils.removeElement(COL_NAMES, "quality");
	static final String LABEL_COL_NAME = "quality";
	private static final String PREDICTION_COL_NAME = "pred";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_2();

		c_3();

		c_4();

		c_5();

	}

	static void c_2() throws Exception {

		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_FILE)
			.setSchemaStr(Utils.generateSchemaString(COL_NAMES, COL_TYPES))
			.setFieldDelimiter(";")
			.setIgnoreFirstLine(true);

		source.lazyPrint(5);

		source.link(new CorrelationBatchOp().lazyPrintCorrelation());

		source
			.groupBy(LABEL_COL_NAME, LABEL_COL_NAME + ", COUNT(*) AS cnt")
			.orderBy(LABEL_COL_NAME, 100)
			.lazyPrint(-1);

		BatchOperator.execute();

		Utils.splitTrainTestIfNotExist(
			source,
			DATA_DIR + TRAIN_FILE,
			DATA_DIR + TEST_FILE,
			0.8
		);

	}

	static void c_3() throws Exception {
		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new LinearRegression()
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintTrainInfo()
			.enableLazyPrintModelInfo()
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("LinearRegression")
			);

		new LassoRegression()
			.setLambda(0.05)
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintTrainInfo()
			.enableLazyPrintModelInfo("< LASSO model >")
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("LassoRegression")
			);

		BatchOperator.execute();
	}

	static void c_4() throws Exception {
		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		new DecisionTreeRegressor()
			.setFeatureCols(FEATURE_COL_NAMES)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("DecisionTreeRegressor")
			);
		BatchOperator.execute();

		for (int numTrees : new int[] {2, 4, 8, 16, 32, 64, 128}) {
			new RandomForestRegressor()
				.setNumTrees(numTrees)
				.setFeatureCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionCol(PREDICTION_COL_NAME)
				.fit(train_data)
				.transform(test_data)
				.link(
					new EvalRegressionBatchOp()
						.setLabelCol(LABEL_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.lazyPrintMetrics("RandomForestRegressor - " + numTrees)
				);
			BatchOperator.execute();
		}

	}

	static void c_5() throws Exception {
		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_FILE);

		for (int numTrees : new int[] {16, 32, 64, 128, 256, 512}) {
			new GbdtRegressor()
				.setLearningRate(0.05)
				.setMaxLeaves(256)
				.setFeatureSubsamplingRatio(0.3)
				.setMinSamplesPerLeaf(2)
				.setMaxDepth(100)
				.setNumTrees(numTrees)
				.setFeatureCols(FEATURE_COL_NAMES)
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionCol(PREDICTION_COL_NAME)
				.fit(train_data)
				.transform(test_data)
				.link(
					new EvalRegressionBatchOp()
						.setLabelCol(LABEL_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.lazyPrintMetrics("GbdtRegressor - " + numTrees)
				);
			BatchOperator.execute();
		}

	}

}
