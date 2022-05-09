package com.alibaba.alink;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.image.ReadImageToTensorStreamOp;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.TensorToVector;

import java.io.File;

public class Chap26 {

	static final String DATA_DIR = Utils.ROOT_DIR + "dog_cat" + File.separator;
	static final String IMAGE_DIR = DATA_DIR + "train" + File.separator;

	static final String TRAIN_96_FILE = "train_96.ak";
	static final String TEST_96_FILE = "test_96.ak";

	static final String TRAIN_32_FILE = "train_32.ak";
	static final String TEST_32_FILE = "test_32.ak";

	static final String MODEL_CNN_FILE = "model_cnn.ak";
	static final String MODEL_EFNET_FILE = "model_efnet.ak";
	static final String MODEL_EFNET_OFFLINE_FILE = "model_efnet_offline.ak";

	static final String PREDICTION_COL = "pred";
	static final String PREDICTION_DETAIL_COL = "pred_info";

	public static void main(String[] args) throws Exception {

		c_1();

		c_2();

		c_3();

	}

	static void c_1() throws Exception {

		if (new File(DATA_DIR + TRAIN_96_FILE).exists()
			&& new File(DATA_DIR + TRAIN_32_FILE).exists()
		) {
			return;
		}

		new MemSourceBatchOp(new File(IMAGE_DIR).list(), "relative_path")
			.select("relative_path, REGEXP_EXTRACT(relative_path, '(dog|cat)') AS label")
			.lazyPrint(10)
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + "list_all.ak")
					.setOverwriteSink(true)
			);
		BatchOperator.execute();

		Utils.splitTrainTestIfNotExist(
			new AkSourceBatchOp().setFilePath(DATA_DIR + "list_all.ak"),
			DATA_DIR + "list_train.ak",
			DATA_DIR + "list_test.ak",
			0.9
		);

		new AkSourceStreamOp()
			.setFilePath(DATA_DIR + "list_train.ak")
			.link(
				new ReadImageToTensorStreamOp()
					.setRelativeFilePathCol("relative_path")
					.setRootFilePath(IMAGE_DIR)
					.setImageWidth(32)
					.setImageHeight(32)
					.setOutputCol("tensor")
			)
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TRAIN_32_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceStreamOp()
			.setFilePath(DATA_DIR + "list_train.ak")
			.link(
				new ReadImageToTensorStreamOp()
					.setRelativeFilePathCol("relative_path")
					.setRootFilePath(IMAGE_DIR)
					.setImageWidth(96)
					.setImageHeight(96)
					.setOutputCol("tensor")
			)
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TRAIN_96_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceStreamOp()
			.setFilePath(DATA_DIR + "list_test.ak")
			.link(
				new ReadImageToTensorStreamOp()
					.setRelativeFilePathCol("relative_path")
					.setRootFilePath(IMAGE_DIR)
					.setImageWidth(32)
					.setImageHeight(32)
					.setOutputCol("tensor")
			)
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TEST_32_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceStreamOp()
			.setFilePath(DATA_DIR + "list_test.ak")
			.link(
				new ReadImageToTensorStreamOp()
					.setRelativeFilePathCol("relative_path")
					.setRootFilePath(IMAGE_DIR)
					.setImageWidth(96)
					.setImageHeight(96)
					.setOutputCol("tensor")
			)
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TEST_96_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

	}

	static void c_2() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_32_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_32_FILE);

		lr(train_set, test_set);

		cnn(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void lr(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		new Pipeline()
			.add(
				new TensorToVector()
					.setSelectedCol("tensor")
					.setReservedCols("label")
			)
			.add(
				new LogisticRegression()
					.setVectorCol("tensor")
					.setLabelCol("label")
					.setPredictionCol(PREDICTION_COL)
					.setPredictionDetailCol(PREDICTION_DETAIL_COL)
			)
			.fit(train_set)
			.transform(test_set)
			.link(
				new EvalBinaryClassBatchOp()
					.setLabelCol("label")
					.setPredictionDetailCol(PREDICTION_DETAIL_COL)
					.lazyPrintMetrics()
			);
		BatchOperator.execute();

	}

	public static void cnn(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		if (!new File(DATA_DIR + MODEL_CNN_FILE).exists()) {
			train_set
				.link(
					new KerasSequentialClassifierTrainBatchOp()
						.setTensorCol("tensor")
						.setLabelCol("label")
						.setLayers(
							"Conv2D(32, kernel_size=(3, 3), activation='relu')",
							"MaxPooling2D(pool_size=(2, 2))",
							"Conv2D(64, kernel_size=(3, 3), activation='relu')",
							"MaxPooling2D(pool_size=(2, 2))",
							"Flatten()",
							"Dropout(0.5)"
						)
						.setNumEpochs(50)
						.setSaveCheckpointsEpochs(2.0)
						.setValidationSplit(0.1)
						.setSaveBestOnly(true)
						.setBestMetric("auc")
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + MODEL_CNN_FILE)
				);
			BatchOperator.execute();
		}

		new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol(PREDICTION_COL)
			.setPredictionDetailCol(PREDICTION_DETAIL_COL)
			.setReservedCols("relative_path", "label")
			.linkFrom(
				new AkSourceBatchOp().setFilePath(DATA_DIR + MODEL_CNN_FILE),
				test_set
			)
			.lazyPrint(10)
			.lazyPrintStatistics()
			.link(
				new EvalBinaryClassBatchOp()
					.setLabelCol("label")
					.setPredictionDetailCol(PREDICTION_DETAIL_COL)
					.lazyPrintMetrics()
			);
		BatchOperator.execute();
	}

	static void c_3() throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		AkSourceBatchOp train_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TRAIN_96_FILE);
		AkSourceBatchOp test_set = new AkSourceBatchOp().setFilePath(DATA_DIR + TEST_96_FILE);

		efficientnet(train_set, test_set);

		efficientnet_offline(train_set, test_set);

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

	public static void efficientnet(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		if (!new File(DATA_DIR + MODEL_EFNET_FILE).exists()) {
			train_set
				.link(
					new KerasSequentialClassifierTrainBatchOp()
						.setTensorCol("tensor")
						.setLabelCol("label")
						.setLayers(
							"hub.KerasLayer('https://hub.tensorflow.google"
								+ ".cn/google/efficientnet/b0/classification/1')",
							"Flatten()"
						)
						.setNumEpochs(5)
						.setIntraOpParallelism(1)
						.setSaveCheckpointsEpochs(0.5)
						.setValidationSplit(0.1)
						.setSaveBestOnly(true)
						.setBestMetric("auc")
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + MODEL_EFNET_FILE)
				);
			BatchOperator.execute();
		}

		new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol(PREDICTION_COL)
			.setPredictionDetailCol(PREDICTION_DETAIL_COL)
			.setReservedCols("relative_path", "label")
			.linkFrom(
				new AkSourceBatchOp().setFilePath(DATA_DIR + MODEL_EFNET_FILE),
				test_set
			)
			.lazyPrint(10)
			.lazyPrintStatistics()
			.link(
				new EvalBinaryClassBatchOp()
					.setLabelCol("label")
					.setPredictionDetailCol(PREDICTION_DETAIL_COL)
					.lazyPrintMetrics()
			);
		BatchOperator.execute();
	}

	public static void efficientnet_offline(BatchOperator <?> train_set, BatchOperator <?> test_set) throws Exception {
		BatchOperator.setParallelism(1);

		if (!new File(DATA_DIR + MODEL_EFNET_OFFLINE_FILE).exists()) {
			train_set
				.link(
					new KerasSequentialClassifierTrainBatchOp()
						.setTensorCol("tensor")
						.setLabelCol("label")
						.setLayers(
							"hub.KerasLayer('" + DATA_DIR + "1')",
							"Flatten()"
						)
						.setNumEpochs(5)
						.setIntraOpParallelism(1)
						.setSaveCheckpointsEpochs(0.5)
						.setValidationSplit(0.1)
						.setSaveBestOnly(true)
						.setBestMetric("auc")
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + MODEL_EFNET_OFFLINE_FILE)
				);
			BatchOperator.execute();
		}

		new KerasSequentialClassifierPredictBatchOp()
			.setPredictionCol(PREDICTION_COL)
			.setPredictionDetailCol(PREDICTION_DETAIL_COL)
			.setReservedCols("relative_path", "label")
			.linkFrom(
				new AkSourceBatchOp().setFilePath(DATA_DIR + MODEL_EFNET_OFFLINE_FILE),
				test_set
			)
			.lazyPrint(10)
			.lazyPrintStatistics()
			.link(
				new EvalBinaryClassBatchOp()
					.setLabelCol("label")
					.setPredictionDetailCol(PREDICTION_DETAIL_COL)
					.lazyPrintMetrics()
			);
		BatchOperator.execute();
	}

}