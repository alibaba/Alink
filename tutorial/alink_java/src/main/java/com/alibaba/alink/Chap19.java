package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.VectorToColumnsBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.feature.PcaPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.PcaTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.feature.HasCalculationType.CalculationType;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.KnnClassifier;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.feature.PCA;
import com.alibaba.alink.pipeline.feature.PCAModel;

import java.io.File;

public class Chap19 {

	static final String DATA_DIR = Utils.ROOT_DIR + "mnist" + File.separator;

	static final String DENSE_TRAIN_FILE = "dense_train.ak";
	static final String DENSE_TEST_FILE = "dense_test.ak";
	static final String SPARSE_TRAIN_FILE = "sparse_train.ak";
	static final String SPARSE_TEST_FILE = "sparse_test.ak";
	static final String PCA_MODEL_FILE = "pca_model.ak";

	static final String VECTOR_COL_NAME = "vec";
	static final String LABEL_COL_NAME = "label";
	static final String PREDICTION_COL_NAME = "pred";

	static final String[] CRIME_COL_NAMES =
		new String[] {"state", "murder", "rape", "robbery", "assault", "burglary", "larceny", "auto"};

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_1();

		c_2();

		c_3();

		c_4();

	}

	static void c_1() throws Exception {

		MemSourceBatchOp source = new MemSourceBatchOp(CRIME_ROWS_DATA, CRIME_COL_NAMES);

		source.lazyPrint(10, "Origin data");

		BatchOperator <?> pca_result = new PCA()
			.setK(4)
			.setSelectedCols("murder", "rape", "robbery", "assault", "burglary", "larceny", "auto")
			.setPredictionCol(VECTOR_COL_NAME)
			.enableLazyPrintModelInfo()
			.fit(source)
			.transform(source)
			.link(
				new VectorToColumnsBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setSchemaStr("prin1 double, prin2 double, prin3 double, prin4 double")
					.setReservedCols("state")
			)
			.lazyPrint(10, "state with principle components");

		pca_result
			.select("state, prin1")
			.orderBy("prin1", 100, false)
			.lazyPrint(-1, "Order by prin1");

		pca_result
			.select("state, prin2")
			.orderBy("prin2", 100, false)
			.lazyPrint(-1, "Order by prin2");

		BatchOperator.execute();

	}

	static void c_2() throws Exception {

		MemSourceBatchOp source = new MemSourceBatchOp(CRIME_ROWS_DATA, CRIME_COL_NAMES);

		Pipeline std_pca = new Pipeline()
			.add(
				new StandardScaler()
					.setSelectedCols("murder", "rape", "robbery", "assault", "burglary", "larceny", "auto")
			)
			.add(
				new PCA()
					.setCalculationType(CalculationType.COV)
					.setK(4)
					.setSelectedCols("murder", "rape", "robbery", "assault", "burglary", "larceny", "auto")
					.setPredictionCol(VECTOR_COL_NAME)
					.enableLazyPrintModelInfo()
			);

		std_pca
			.fit(source)
			.transform(source)
			.link(
				new VectorToColumnsBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setSchemaStr("prin1 double, prin2 double, prin3 double, prin4 double")
					.setReservedCols("state")
			)
			.lazyPrint(10, "state with principle components");
		BatchOperator.execute();

	}

	static void c_3() throws Exception {

		AkSourceBatchOp source = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);

		source
			.link(
				new PcaTrainBatchOp()
					.setK(39)
					.setCalculationType(CalculationType.COV)
					.setVectorCol(VECTOR_COL_NAME)
					.lazyPrintModelInfo()
			)
			.link(
				new AkSinkBatchOp()
					.setFilePath(DATA_DIR + PCA_MODEL_FILE)
					.setOverwriteSink(true)
			);
		BatchOperator.execute();

		BatchOperator <?> pca_result = new PcaPredictBatchOp()
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(VECTOR_COL_NAME)
			.linkFrom(
				new AkSourceBatchOp().setFilePath(DATA_DIR + PCA_MODEL_FILE),
				source
			);

		Stopwatch sw = new Stopwatch();

		KMeans kmeans = new KMeans()
			.setK(10)
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME);

		sw.reset();
		sw.start();
		kmeans
			.fit(source)
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("KMeans")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		kmeans
			.fit(pca_result)
			.transform(pca_result)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("KMeans + PCA")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

	}

	static void c_4() throws Exception {

		AkSourceBatchOp dense_train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + DENSE_TRAIN_FILE);
		AkSourceBatchOp dense_test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + DENSE_TEST_FILE);
		AkSourceBatchOp sparse_train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceBatchOp sparse_test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

		Stopwatch sw = new Stopwatch();

		sw.reset();
		sw.start();
		new KnnClassifier()
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(dense_train_data)
			.transform(dense_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KnnClassifier Dense")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		new KnnClassifier()
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(sparse_train_data)
			.transform(sparse_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KnnClassifier Sparse")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		new Pipeline()
			.add(
				new PCA()
					.setK(39)
					.setCalculationType(CalculationType.COV)
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(VECTOR_COL_NAME)
			)
			.add(
				new KnnClassifier()
					.setK(3)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.fit(dense_train_data)
			.transform(dense_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("Knn with PCA Dense")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		new Pipeline()
			.add(
				new PCA()
					.setK(39)
					.setCalculationType(CalculationType.COV)
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(VECTOR_COL_NAME)
			)
			.add(
				new KnnClassifier()
					.setK(3)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.fit(sparse_train_data)
			.transform(sparse_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("Knn with PCA Sparse")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		new Pipeline()
			.add(
				new PCAModel()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(VECTOR_COL_NAME)
					.setModelData(
						new AkSourceBatchOp()
							.setFilePath(DATA_DIR + PCA_MODEL_FILE)
					)
			)
			.add(
				new KnnClassifier()
					.setK(3)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.fit(dense_train_data)
			.transform(dense_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("Knn PCAModel Dense")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		new Pipeline()
			.add(
				new PCAModel()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(VECTOR_COL_NAME)
					.setModelData(
						new AkSourceBatchOp()
							.setFilePath(DATA_DIR + PCA_MODEL_FILE)
					)
			)
			.add(
				new KnnClassifier()
					.setK(3)
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.fit(sparse_train_data)
			.transform(sparse_test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("Knn PCAModel Sparse")
			);
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

	}

	private static final Row[] CRIME_ROWS_DATA = new Row[] {
		Row.of("ALABAMA", 14.2, 25.2, 96.8, 278.3, 1135.5, 1881.9, 280.7),
		Row.of("ALASKA", 10.8, 51.6, 96.8, 284.0, 1331.7, 3369.8, 753.3),
		Row.of("ARIZONA", 9.5, 34.2, 138.2, 312.3, 2346.1, 4467.4, 439.5),
		Row.of("ARKANSAS", 8.8, 27.6, 83.2, 203.4, 972.6, 1862.1, 183.4),
		Row.of("CALIFORNIA", 11.5, 49.4, 287.0, 358.0, 2139.4, 3499.8, 663.5),
		Row.of("COLORADO", 6.3, 42.0, 170.7, 292.9, 1935.2, 3903.2, 477.1),
		Row.of("CONNECTICUT", 4.2, 16.8, 129.5, 131.8, 1346.0, 2620.7, 593.2),
		Row.of("DELAWARE", 6.0, 24.9, 157.0, 194.2, 1682.6, 3678.4, 467.0),
		Row.of("FLORIDA", 10.2, 39.6, 187.9, 449.1, 1859.9, 3840.5, 351.4),
		Row.of("GEORGIA", 11.7, 31.1, 140.5, 256.5, 1351.1, 2170.2, 297.9),
		Row.of("HAWAII", 7.2, 25.5, 128.0, 64.1, 1911.5, 3920.4, 489.4),
		Row.of("IDAHO", 5.5, 19.4, 39.6, 172.5, 1050.8, 2599.6, 237.6),
		Row.of("ILLINOIS", 9.9, 21.8, 211.3, 209.0, 1085.0, 2828.5, 528.6),
		Row.of("INDIANA", 7.4, 26.5, 123.2, 153.5, 1086.2, 2498.7, 377.4),
		Row.of("IOWA", 2.3, 10.6, 41.2, 89.8, 812.5, 2685.1, 219.9),
		Row.of("KANSAS", 6.6, 22.0, 100.7, 180.5, 1270.4, 2739.3, 244.3),
		Row.of("KENTUCKY", 10.1, 19.1, 81.1, 123.3, 872.2, 1662.1, 245.4),
		Row.of("LOUISIANA", 15.5, 30.9, 142.9, 335.5, 1165.5, 2469.9, 337.7),
		Row.of("MAINE", 2.4, 13.5, 38.7, 170.0, 1253.1, 2350.7, 246.9),
		Row.of("MARYLAND", 8.0, 34.8, 292.1, 358.9, 1400.0, 3177.7, 428.5),
		Row.of("MASSACHUSETTS", 3.1, 20.8, 169.1, 231.6, 1532.2, 2311.3, 1140.1),
		Row.of("MICHIGAN", 9.3, 38.9, 261.9, 274.6, 1522.7, 3159.0, 545.5),
		Row.of("MINNESOTA", 2.7, 19.5, 85.9, 85.8, 1134.7, 2559.3, 343.1),
		Row.of("MISSISSIPPI", 14.3, 19.6, 65.7, 189.1, 915.6, 1239.9, 144.4),
		Row.of("MISSOURI", 9.6, 28.3, 189.0, 233.5, 1318.3, 2424.2, 378.4),
		Row.of("MONTANA", 5.4, 16.7, 39.2, 156.8, 804.9, 2773.2, 309.2),
		Row.of("NEBRASKA", 3.9, 18.1, 64.7, 112.7, 760.0, 2316.1, 249.1),
		Row.of("NEVADA", 15.8, 49.1, 323.1, 355.0, 2453.1, 4212.6, 559.2),
		Row.of("NEW HAMPSHIRE", 3.2, 10.7, 23.2, 76.0, 1041.7, 2343.9, 293.4),
		Row.of("NEW JERSEY", 5.6, 21.0, 180.4, 185.1, 1435.8, 2774.5, 511.5),
		Row.of("NEW MEXICO", 8.8, 39.1, 109.6, 343.4, 1418.7, 3008.6, 259.5),
		Row.of("NEW YORK", 10.7, 29.4, 472.6, 319.1, 1728.0, 2782.0, 745.8),
		Row.of("NORTH CAROLINA", 10.6, 17.0, 61.3, 318.3, 1154.1, 2037.8, 192.1),
		Row.of("NORTH DAKOTA", 0.9, 9.0, 13.3, 43.8, 446.1, 1843.0, 144.7),
		Row.of("OHIO", 7.8, 27.3, 190.5, 181.1, 1216.0, 2696.8, 400.4),
		Row.of("OKLAHOMA", 8.6, 29.2, 73.8, 205.0, 1288.2, 2228.1, 326.8),
		Row.of("OREGON", 4.9, 39.9, 124.1, 286.9, 1636.4, 3506.1, 388.9),
		Row.of("PENNSYLVANIA", 5.6, 19.0, 130.3, 128.0, 877.5, 1624.1, 333.2),
		Row.of("RHODE ISLAND", 3.6, 10.5, 86.5, 201.0, 1489.5, 2844.1, 791.4),
		Row.of("SOUTH CAROLINA", 11.9, 33.0, 105.9, 485.3, 1613.6, 2342.4, 245.1),
		Row.of("SOUTH DAKOTA", 2.0, 13.5, 17.9, 155.7, 570.5, 1704.4, 147.5),
		Row.of("TENNESSEE", 10.1, 29.7, 145.8, 203.9, 1259.7, 1776.5, 314.0),
		Row.of("TEXAS", 13.3, 33.8, 152.4, 208.2, 1603.1, 2988.7, 397.6),
		Row.of("UTAH", 3.5, 20.3, 68.8, 147.3, 1171.6, 3004.6, 334.5),
		Row.of("VERMONT", 1.4, 15.9, 30.8, 101.2, 1348.2, 2201.0, 265.2),
		Row.of("VIRGINIA", 9.0, 23.3, 92.1, 165.7, 986.2, 2521.2, 226.7),
		Row.of("WASHINGTON", 4.3, 39.6, 106.2, 224.8, 1605.6, 3386.9, 360.3),
		Row.of("WEST VIRGINIA", 6.0, 13.2, 42.2, 90.9, 597.4, 1341.7, 163.3),
		Row.of("WISCONSIN", 2.8, 12.9, 52.2, 63.7, 846.9, 2614.2, 220.7),
		Row.of("WYOMING", 5.4, 21.9, 39.7, 173.9, 811.6, 2772.2, 282.0)
	};

}
