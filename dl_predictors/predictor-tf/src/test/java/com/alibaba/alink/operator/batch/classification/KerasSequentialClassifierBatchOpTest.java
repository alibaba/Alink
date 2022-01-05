package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KerasSequentialClassifierBatchOpTest {

	public void testConfig(Integer parallelism, Integer numPSs, String checkpointFilePath) throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(parallelism);

		BatchOperator<?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label int");

		ToTensorBatchOp toTensorBatchOp = new ToTensorBatchOp()
			.setSelectedCol("tensor")
			.setTensorDataType("DOUBLE")
			.setTensorShape(200, 3)
			.linkFrom(source);

		KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(new String[] {
				"Conv1D(256, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Dropout(0.1)",
				"MaxPooling1D(pool_size=8)",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Flatten()"
			})
			.setOptimizer("Adam()")
			.setNumPSs(numPSs)
			.setCheckpointFilePath(checkpointFilePath)
			.setNumEpochs(1)
			.linkFrom(toTensorBatchOp);

		KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
		BatchOperator.setParallelism(savedParallelism);
	}

	@Category(DLTest.class)
	@Test
	public void testSingleWorker() throws Exception {
		testConfig(1, null, null);
	}

	@Category(DLTest.class)
	@Test
	public void testSingleWorkerModelDir() throws Exception {
		testConfig(1, null, PythonFileUtils.createTempDir("keras_sequential_train_").toString());
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersAllReduce() throws Exception {
		testConfig(3, 0, null);
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersAllReduceModelDir() throws Exception {
		testConfig(3, 0, PythonFileUtils.createTempDir("keras_sequential_train_").toString());
	}

	@Test
	public void testMultiWorkersPS() throws Exception {
		testConfig(3, null, null);
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersPSModelDir() throws Exception {
		testConfig(3, null, PythonFileUtils.createTempDir("keras_sequential_train_").toString());
	}

	@Category(DLTest.class)
	@Test
	public void testInputWithTensorType() throws Exception {
		Random random = new Random();

		int n = 1000;
		int nTimesteps = 200;
		int nvars = 3;

		List <Row> rows = new ArrayList <>();
		for (int k = 0; k < n; k += 1) {
			double[][] xArr = new double[nTimesteps][nvars];
			for (int i = 0; i < nTimesteps; i += 1) {
				for (int j = 0; j < nvars; j += 1) {
					xArr[i][j] = random.nextFloat();
				}
			}
			DoubleTensor x = new DoubleTensor(xArr);
			int label = random.nextInt(2);
			rows.add(Row.of(x, label));
		}

			BatchOperator <?> source = new MemSourceBatchOp(rows,
				"tensor DOUBLE_TENSOR, label int");

		KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(new String[] {
				"Conv1D(256, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Dropout(0.1)",
				"MaxPooling1D(pool_size=8)",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Flatten()"
			})
			.setOptimizer("Adam()")
			.setNumEpochs(1)
			.linkFrom(source);

		KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
	}

	@Category(DLTest.class)
	@Test
	public void testTFHubLayer() throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);

		Random random = new Random();

		int n = 1000;
		int nTimesteps = 96;
		int nvars = 3;

		List <Row> rows = new ArrayList <>();
		for (int nn = 0; nn < n; nn += 1) {
			double[][][] xArr = new double[nTimesteps][nTimesteps][nvars];
			for (int i = 0; i < nTimesteps; i += 1) {
				for (int j = 0; j < nTimesteps; j += 1) {
					for (int k = 0; k < nvars; k += 1) {
						xArr[i][j][k] = random.nextFloat();
					}
				}
			}
			DoubleTensor x = new DoubleTensor(xArr);
			int label = random.nextInt(2);
			rows.add(Row.of(x, label));
		}

		BatchOperator <?> source = new MemSourceBatchOp(rows,
			"tensor DOUBLE_TENSOR, label int");

		KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(
				"Reshape((96, 96, 3))",
				//"hub.KerasLayer('https://tfhub.dev/google/imagenet/mobilenet_v2_075_96/classification/5',
				// input_shape=(96,96,3))",
				"hub.KerasLayer('https://hub.tensorflow.google.cn/tensorflow/efficientnet/b0/classification/1')",
				"Flatten()")
			.setCheckpointFilePath(PythonFileUtils.createTempDir("keras_sequential_train_").toString())
			.setNumEpochs(1)
			.linkFrom(source);

		KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
		BatchOperator.setParallelism(savedParallelism);
	}

	@Category(DLTest.class)
	@Test
	public void testTFHubLayerStringTensor() throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);
		Random random = new Random();
		int n = 1000;
		List <Row> rows = new ArrayList <>();
		for (int i = 0; i < n; i += 1) {
			int length = random.nextInt(8) + 1;
			String arr = RandomStringUtils.randomAlphanumeric(length);
			int label = random.nextInt(2);
			rows.add(Row.of(new StringTensor(arr), label));
		}

		BatchOperator <?> source = new MemSourceBatchOp(rows, "tensor STRING_TENSOR, label int");

		KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(
				"hub.KerasLayer('https://tfhub.dev/google/nnlm-de-dim50/2', input_shape=[], dtype=tf.string)",
				"Flatten()")
			.setCheckpointFilePath(PythonFileUtils.createTempDir("keras_sequential_train_").toString())
			.setNumEpochs(1)
			.linkFrom(source);

		KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
		BatchOperator.setParallelism(savedParallelism);
	}

	@Category(DLTest.class)
	@Test
	public void testValidation() throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);

		BatchOperator<?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/random_tensor.csv")
			.setSchemaStr("tensor string, label int");

		source = source.sampleWithSize(1000, true);

		ToTensorBatchOp toTensorBatchOp = new ToTensorBatchOp()
			.setSelectedCol("tensor")
			.setTensorDataType("DOUBLE")
			.setTensorShape(200, 3)
			.linkFrom(source);

		KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
			.setTensorCol("tensor")
			.setLabelCol("label")
			.setLayers(new String[] {
				"Conv1D(256, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Dropout(0.1)",
				"MaxPooling1D(pool_size=8)",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Conv1D(128, 5, padding='same', activation='relu')",
				"Flatten()"
			})
			.setOptimizer("Adam()")
			.setNumEpochs(5)
			.setValidationSplit(0.2)
			.setBatchSize(32)
			.setSaveCheckpointsEpochs(0.5)
			//.setSaveCheckpointsSecs(10.)
			.setSaveBestOnly(true)
			.setBestMetric("binary_accuracy")
			.linkFrom(toTensorBatchOp);

		KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols("label")
			.linkFrom(trainBatchOp, source);
		predictBatchOp.lazyPrint(10);
		BatchOperator.execute();
		BatchOperator.setParallelism(savedParallelism);
	}
}
