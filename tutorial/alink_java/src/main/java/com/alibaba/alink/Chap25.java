package com.alibaba.alink;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassifier;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.VectorToTensor;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorFunction;
import com.alibaba.alink.pipeline.regression.KerasSequentialRegressor;
import com.alibaba.alink.pipeline.regression.LinearRegression;

public class Chap25 {

	public static void main(String[] args) throws Exception {

		c_1_3();

		c_2();

		c_3();

	}

	static void c_1_3() throws Exception {
		System.out.println(AlinkGlobalConfiguration.getPluginDir());

		System.out.print("Auto Plugin Download : ");
		System.out.println(AlinkGlobalConfiguration.getAutoPluginDownload());

		PluginDownloader downloader = AlinkGlobalConfiguration.getPluginDownloader();

		System.out.println(downloader.listAvailablePlugins());
	}

	static void c_2() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE);

		softmax(train_set, test_set);

		dnn(train_set, test_set);

		cnn(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void softmax(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new Pipeline()
			.add(
				new Softmax()
					.setVectorCol("vec")
					.setLabelCol("label")
					.setPredictionCol("pred")
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol("label")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}

	public static void dnn(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new Pipeline()
			.add(
				new VectorFunction()
					.setSelectedCol("vec")
					.setFuncName("Scale")
					.setWithVariable(1.0 / 255.0)
			)
			.add(
				new VectorToTensor()
					.setTensorDataType("float")
					.setSelectedCol("vec")
					.setOutputCol("tensor")
					.setReservedCols("label")
			)
			.add(
				new KerasSequentialClassifier()
					.setTensorCol("tensor")
					.setLabelCol("label")
					.setPredictionCol("pred")
					.setLayers(
						"Dense(256, activation='relu')",
						"Dense(128, activation='relu')"
					)
					.setNumEpochs(50)
					.setBatchSize(512)
					.setValidationSplit(0.1)
					.setSaveBestOnly(true)
					.setBestMetric("sparse_categorical_accuracy")
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol("label")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}

	public static void cnn(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new Pipeline()
			.add(
				new VectorFunction()
					.setSelectedCol("vec")
					.setFuncName("Scale")
					.setWithVariable(1.0 / 255.0)
			)
			.add(
				new VectorToTensor()
					.setTensorDataType("float")
					.setTensorShape(28, 28)
					.setSelectedCol("vec")
					.setOutputCol("tensor")
					.setReservedCols("label")
			)
			.add(
				new KerasSequentialClassifier()
					.setTensorCol("tensor")
					.setLabelCol("label")
					.setPredictionCol("pred")
					.setLayers(
						"Reshape((28, 28, 1))",
						"Conv2D(32, kernel_size=(3, 3), activation='relu')",
						"MaxPooling2D(pool_size=(2, 2))",
						"Conv2D(64, kernel_size=(3, 3), activation='relu')",
						"MaxPooling2D(pool_size=(2, 2))",
						"Flatten()",
						"Dropout(0.5)"
					)
					.setNumEpochs(20)
					.setValidationSplit(0.1)
					.setSaveBestOnly(true)
					.setBestMetric("sparse_categorical_accuracy")
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol("label")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}

	static void c_3() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(Chap16.DATA_DIR + Chap16.TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(Chap16.DATA_DIR + Chap16.TEST_FILE);

		linearReg(train_set, test_set);

		dnnReg(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void linearReg(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new LinearRegression()
			.setFeatureCols(Chap16.FEATURE_COL_NAMES)
			.setLabelCol("quality")
			.setPredictionCol("pred")
			.enableLazyPrintModelInfo()
			.fit(train_set)
			.transform(test_set)
			.lazyPrintStatistics()
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol("quality")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);
		BatchOperator.execute();
	}

	public static void dnnReg(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new Pipeline()
			.add(
				new StandardScaler()
					.setSelectedCols(Chap16.FEATURE_COL_NAMES)
			)
			.add(
				new VectorAssembler()
					.setSelectedCols(Chap16.FEATURE_COL_NAMES)
					.setOutputCol("vec")
			)
			.add(
				new VectorToTensor()
					.setSelectedCol("vec")
					.setOutputCol("tensor")
					.setReservedCols("quality")
			)
			.add(
				new KerasSequentialRegressor()
					.setTensorCol("tensor")
					.setLabelCol("quality")
					.setPredictionCol("pred")
					.setLayers(
						"Dense(64, activation='relu')",
						"Dense(64, activation='relu')",
						"Dense(64, activation='relu')",
						"Dense(64, activation='relu')",
						"Dense(64, activation='relu')"
					)
					.setNumEpochs(20)
			)
			.fit(train_set)
			.transform(test_set)
			.lazyPrintStatistics()
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol("quality")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);
		BatchOperator.execute();

	}
}