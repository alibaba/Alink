package com.alibaba.alink;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.VectorToTensorBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorFunctionBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.pytorch.TorchModelPredictBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFSavedModelPredictBatchOp;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.VectorToTensorStreamOp;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorFunctionStreamOp;
import com.alibaba.alink.operator.stream.pytorch.TorchModelPredictStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp;
import com.alibaba.alink.operator.stream.utils.UDFStreamOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.classification.KerasSequentialClassifier;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.VectorToTensor;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorFunction;
import com.alibaba.alink.pipeline.pytorch.TorchModelPredictor;
import com.alibaba.alink.pipeline.regression.KerasSequentialRegressor;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.tensorflow.TFSavedModelPredictor;

public class Chap25 {

	static String PIPELINE_TF_MODEL = "pipeline_tf_model.ak";
	static String PIPELINE_PYTORCH_MODEL = "pipeline_pytorch_model.ak";

	public static void main(String[] args) throws Exception {

		StreamOperator.setParallelism(4);
		BatchOperator.setParallelism(4);

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		c_1_3();

		c_2();

		c_3();

		c_4_2();

		c_4_3();

		c_4_4();

		c_4_5();

		c_5_2();

		c_5_3();

		c_5_4();

		c_5_5();
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

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE);

		softmax(train_set, test_set);

		dnn(train_set, test_set);

		cnn(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void softmax(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {

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
					.setNumWorkers(1)
					.setNumPSs(0)
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
					.setNumWorkers(1)
					.setNumPSs(0)
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

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(Chap16.DATA_DIR + Chap16.TRAIN_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(Chap16.DATA_DIR + Chap16.TEST_FILE);

		linearReg(train_set, test_set);

		dnnReg(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void linearReg(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {

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
					.setNumWorkers(1)
					.setNumPSs(0)
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

	static void c_4_2() throws Exception {

		new AkSourceBatchOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			.link(
				new VectorFunctionBatchOp()
					.setSelectedCol("vec")
					.setFuncName("Scale")
					.setWithVariable(1.0 / 255.0)
			)
			.link(
				new VectorToTensorBatchOp()
					.setTensorDataType("float")
					.setTensorShape(1, 28, 28, 1)
					.setSelectedCol("vec")
					.setOutputCol("input_1")
					.setReservedCols("label")
			)
			.link(
				new TFSavedModelPredictBatchOp()
					.setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_tf.zip")
					.setSelectedCols("input_1")
					.setOutputSchemaStr("output_1 FLOAT_TENSOR")
			)
			.lazyPrint(3)
			.link(
				new UDFBatchOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.lazyPrint(3)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol("label")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}

	static void c_4_3() throws Exception {

		new AkSourceStreamOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			.link(
				new VectorFunctionStreamOp()
					.setSelectedCol("vec")
					.setFuncName("Scale")
					.setWithVariable(1.0 / 255.0)
			)
			.link(
				new VectorToTensorStreamOp()
					.setTensorDataType("float")
					.setTensorShape(1, 28, 28, 1)
					.setSelectedCol("vec")
					.setOutputCol("input_1")
					.setReservedCols("label")
			)
			.link(
				new TFSavedModelPredictStreamOp()
					.setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_tf.zip")
					.setSelectedCols("input_1")
					.setOutputSchemaStr("output_1 FLOAT_TENSOR")
			)
			.link(
				new UDFStreamOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.sample(0.001)
			.print();

		StreamOperator.execute();
	}

	static void c_4_4() throws Exception {

		new PipelineModel(
			new VectorFunction()
				.setSelectedCol("vec")
				.setFuncName("Scale")
				.setWithVariable(1.0 / 255.0),
			new VectorToTensor()
				.setTensorDataType("float")
				.setTensorShape(1, 28, 28, 1)
				.setSelectedCol("vec")
				.setOutputCol("input_1")
				.setReservedCols("label"),
			new TFSavedModelPredictor()
				.setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_tf.zip")
				.setSelectedCols("input_1")
				.setOutputSchemaStr("output_1 FLOAT_TENSOR")
		).save(Chap13.DATA_DIR + PIPELINE_TF_MODEL, true);
		BatchOperator.execute();

		PipelineModel
			.load(Chap13.DATA_DIR + PIPELINE_TF_MODEL)
			.transform(
				new AkSourceStreamOp()
					.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			)
			.link(
				new UDFStreamOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.sample(0.001)
			.print();
		StreamOperator.execute();
	}

	static void c_4_5() throws Exception {

		AkSourceBatchOp source = new AkSourceBatchOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE);

		System.out.println(source.getSchema());

		Row row = source.firstN(1).collect().get(0);

		LocalPredictor localPredictor
			= new LocalPredictor(Chap13.DATA_DIR + PIPELINE_TF_MODEL, "vec string, label int");

		System.out.println(localPredictor.getOutputSchema());

		Row r = localPredictor.map(row);
		System.out.println(r.getField(0).toString() + " | " + r.getField(2).toString());
	}

	static void c_5_2() throws Exception {

		new AkSourceBatchOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			.link(
				new VectorToTensorBatchOp()
					.setTensorDataType("float")
					.setTensorShape(1, 1, 28, 28)
					.setSelectedCol("vec")
					.setOutputCol("tensor")
					.setReservedCols("label")
			)
			.link(
				new TorchModelPredictBatchOp()
					.setModelPath(
						"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_pytorch.pt")
					.setSelectedCols("tensor")
					.setOutputSchemaStr("output_1 FLOAT_TENSOR")
			)
			.lazyPrint(3)
			.link(
				new UDFBatchOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.lazyPrint(3)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol("label")
					.setPredictionCol("pred")
					.lazyPrintMetrics()
			);

		BatchOperator.execute();
	}

	static void c_5_3() throws Exception {

		new AkSourceStreamOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			.link(
				new VectorToTensorStreamOp()
					.setTensorDataType("float")
					.setTensorShape(1, 1, 28, 28)
					.setSelectedCol("vec")
					.setOutputCol("tensor")
					.setReservedCols("label")
			)
			.link(
				new TorchModelPredictStreamOp()
					.setModelPath(
						"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_pytorch.pt")
					.setSelectedCols("tensor")
					.setOutputSchemaStr("output_1 FLOAT_TENSOR")
			)
			.link(
				new UDFStreamOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.sample(0.001)
			.print();

		StreamOperator.execute();
	}

	static void c_5_4() throws Exception {

		new PipelineModel(
			new VectorToTensor()
				.setTensorDataType("float")
				.setTensorShape(1, 1, 28, 28)
				.setSelectedCol("vec")
				.setOutputCol("tensor")
				.setReservedCols("label"),
			new TorchModelPredictor()
				.setModelPath(
					"https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_pytorch.pt")
				.setSelectedCols("tensor")
				.setOutputSchemaStr("output_1 FLOAT_TENSOR")
		).save(Chap13.DATA_DIR + PIPELINE_PYTORCH_MODEL, true);
		BatchOperator.execute();

		PipelineModel
			.load(Chap13.DATA_DIR + PIPELINE_PYTORCH_MODEL)
			.transform(
				new AkSourceStreamOp()
					.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE)
			)
			.link(
				new UDFStreamOp()
					.setFunc(new GetMaxIndex())
					.setSelectedCols("output_1")
					.setOutputCol("pred")
			)
			.sample(0.001)
			.print();
		StreamOperator.execute();
	}

	static void c_5_5() throws Exception {

		AkSourceBatchOp source = new AkSourceBatchOp()
			.setFilePath(Chap13.DATA_DIR + Chap13.DENSE_TEST_FILE);

		System.out.println(source.getSchema());

		Row row = source.firstN(1).collect().get(0);

		LocalPredictor localPredictor
			= new LocalPredictor(Chap13.DATA_DIR + PIPELINE_PYTORCH_MODEL, "vec string, label int");

		System.out.println(localPredictor.getOutputSchema());

		Row r = localPredictor.map(row);
		System.out.println(r.getField(0).toString() + " | " + r.getField(2).toString());
	}

	public static class GetMaxIndex extends ScalarFunction {

		public int eval(FloatTensor tensor) {
			int k = 0;
			float max = tensor.getFloat(0, 0);
			for (int i = 1; i < 10; i++) {
				if (tensor.getFloat(0, i) > max) {
					k = i;
					max = tensor.getFloat(0, i);
				}
			}
			return k;
		}
	}

}