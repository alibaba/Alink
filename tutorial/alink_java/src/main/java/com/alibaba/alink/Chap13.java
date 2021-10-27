package com.alibaba.alink;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType;
import com.alibaba.alink.params.shared.tree.HasIndividualTreeType.TreeType;
import com.alibaba.alink.pipeline.classification.DecisionTreeClassifier;
import com.alibaba.alink.pipeline.classification.KnnClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import com.alibaba.alink.pipeline.classification.OneVsRest;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.classification.Softmax;
import com.alibaba.alink.pipeline.dataproc.format.VectorToColumns;
import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

public class Chap13 {

	static final String DATA_DIR = Utils.ROOT_DIR + "mnist" + File.separator;

	static final String DENSE_TRAIN_FILE = "dense_train.ak";
	static final String DENSE_TEST_FILE = "dense_test.ak";
	static final String SPARSE_TRAIN_FILE = "sparse_train.ak";
	static final String SPARSE_TEST_FILE = "sparse_test.ak";
	static final String TABLE_TRAIN_FILE = "table_train.ak";
	static final String TABLE_TEST_FILE = "table_test.ak";

	static final String VECTOR_COL_NAME = "vec";
	static final String LABEL_COL_NAME = "label";
	static final String PREDICTION_COL_NAME = "id_cluster";

	public static void main(String[] args) throws Exception {

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		BatchOperator.setParallelism(4);

		c_1();

		c_2();

		c_3();

		c_4();

		c_5();

		c_6();

	}

	static void c_1() throws Exception {
		if (!new File(DATA_DIR + SPARSE_TRAIN_FILE).exists()) {
			new MnistGzFileSourceBatchOp
				(
					DATA_DIR + "train-images-idx3-ubyte.gz",
					DATA_DIR + "train-labels-idx1-ubyte.gz",
					true
				)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE)
				);
			new MnistGzFileSourceBatchOp
				(
					DATA_DIR + "t10k-images-idx3-ubyte.gz",
					DATA_DIR + "t10k-labels-idx1-ubyte.gz",
					true
				)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE)
				);
			new MnistGzFileSourceBatchOp
				(
					DATA_DIR + "train-images-idx3-ubyte.gz",
					DATA_DIR + "train-labels-idx1-ubyte.gz",
					false
				)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + DENSE_TRAIN_FILE)
				);
			new MnistGzFileSourceBatchOp
				(
					DATA_DIR + "t10k-images-idx3-ubyte.gz",
					DATA_DIR + "t10k-labels-idx1-ubyte.gz",
					false
				)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + DENSE_TEST_FILE)
				);
			BatchOperator.execute();
		}

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + DENSE_TRAIN_FILE)
			.lazyPrint(1, "MNIST data")
			.link(
				new VectorSummarizerBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.lazyPrintVectorSummary()
			);

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + SPARSE_TRAIN_FILE)
			.lazyPrint(1, "MNIST data")
			.link(
				new VectorSummarizerBatchOp()
					.setSelectedCol(VECTOR_COL_NAME)
					.lazyPrintVectorSummary()
			);

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + SPARSE_TRAIN_FILE)
			.lazyPrintStatistics()
			.groupBy(LABEL_COL_NAME, LABEL_COL_NAME + ", COUNT(*) AS cnt")
			.orderBy("cnt", 100)
			.lazyPrint(-1);

		BatchOperator.execute();

	}

	static void c_2() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

		new Softmax()
			.setVectorCol(VECTOR_COL_NAME)
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

	static void c_3() throws Exception {

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

		BatchOperator.setParallelism(1);

		new OneVsRest()
			.setClassifier(
				new LogisticRegression()
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.setNumClass(10)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("OneVsRest - LogisticRegression")
			);

		new OneVsRest()
			.setClassifier(
				new LinearSvm()
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.setNumClass(10)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("OneVsRest - LinearSvm")
			);

		BatchOperator.execute();

	}

	static void c_4() throws Exception {

		BatchOperator.setParallelism(4);

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

		new MultilayerPerceptronClassifier()
			.setLayers(new int[] {784, 10})
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("MultilayerPerceptronClassifier {784, 10}")
			);
		BatchOperator.execute();

		new MultilayerPerceptronClassifier()
			.setLayers(new int[] {784, 256, 128, 10})
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("MultilayerPerceptronClassifier {784, 256, 128, 10}")
			);
		BatchOperator.execute();
	}

	static void c_5() throws Exception {

		BatchOperator.setParallelism(4);

		if (!new File(DATA_DIR + TABLE_TRAIN_FILE).exists()) {
			AkSourceBatchOp train_sparse = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
			AkSourceBatchOp test_sparse = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

			StringBuilder sbd = new StringBuilder();
			sbd.append("c_0 double");
			for (int i = 1; i < 784; i++) {
				sbd.append(", c_").append(i).append(" double");
			}
			new VectorToColumns()
				.setVectorCol(VECTOR_COL_NAME)
				.setSchemaStr(sbd.toString())
				.setReservedCols(LABEL_COL_NAME)
				.transform(train_sparse)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + TABLE_TRAIN_FILE)
				);
			new VectorToColumns()
				.setVectorCol(VECTOR_COL_NAME)
				.setSchemaStr(sbd.toString())
				.setReservedCols(LABEL_COL_NAME)
				.transform(test_sparse)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + TABLE_TEST_FILE)
				);
			BatchOperator.execute();
		}

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TABLE_TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + TABLE_TEST_FILE);
		final String[] featureColNames = ArrayUtils.removeElement(train_data.getColNames(), LABEL_COL_NAME);

		train_data.lazyPrint(5);

		Stopwatch sw = new Stopwatch();

		for (TreeType treeType : new TreeType[] {TreeType.GINI, TreeType.INFOGAIN, TreeType.INFOGAINRATIO}) {
			sw.reset();
			sw.start();
			new DecisionTreeClassifier()
				.setTreeType(treeType)
				.setFeatureCols(featureColNames)
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionCol(PREDICTION_COL_NAME)
				.enableLazyPrintModelInfo()
				.fit(train_data)
				.transform(test_data)
				.link(
					new EvalMultiClassBatchOp()
						.setLabelCol(LABEL_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.lazyPrintMetrics("DecisionTreeClassifier " + treeType.toString())
				);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());
		}

		for (int numTrees : new int[] {2, 4, 8, 16, 32, 64, 128}) {
			sw.reset();
			sw.start();
			new RandomForestClassifier()
				.setSubsamplingRatio(0.6)
				.setNumTreesOfInfoGain(numTrees)
				.setFeatureCols(featureColNames)
				.setLabelCol(LABEL_COL_NAME)
				.setPredictionCol(PREDICTION_COL_NAME)
				.enableLazyPrintModelInfo()
				.fit(train_data)
				.transform(test_data)
				.link(
					new EvalMultiClassBatchOp()
						.setLabelCol(LABEL_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.lazyPrintMetrics("RandomForestClassifier : " + numTrees)
				);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());
		}

	}

	static void c_6() throws Exception {

		BatchOperator.setParallelism(4);

		AkSourceBatchOp train_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceBatchOp test_data = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TEST_FILE);

		new KnnClassifier()
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KnnClassifier - 3 - EUCLIDEAN")
			);

		BatchOperator.execute();

		new KnnClassifier()
			.setDistanceType(DistanceType.COSINE)
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KnnClassifier - 3 - COSINE")
			);

		BatchOperator.execute();

		new KnnClassifier()
			.setK(7)
			.setVectorCol(VECTOR_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.fit(train_data)
			.transform(test_data)
			.link(
				new EvalMultiClassBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KnnClassifier - 7 - EUCLIDEAN")
			);

		BatchOperator.execute();

	}

	public static class MnistGzFileSourceBatchOp extends BaseSourceBatchOp <MnistGzFileSourceBatchOp> {

		private final String imageGzFile;
		private final String labelGzFile;
		private final boolean isSparse;

		public MnistGzFileSourceBatchOp(String imageGzFile, String labelGzFile, boolean isSparse) {
			super(null, null);
			this.imageGzFile = imageGzFile;
			this.labelGzFile = labelGzFile;
			this.isSparse = isSparse;
		}

		@Override
		protected Table initializeDataSource() {
			try {
				ArrayList <Row> rows = new ArrayList <>();
				String[] images = getImages();
				Integer[] labels = getLabels();
				int n = images.length;
				if (labels.length != n) {
					throw new RuntimeException("The size of images IS NOT EQUAL WITH the size of labels.");
				}
				for (int i = 0; i < n; i++) {
					rows.add(Row.of(images[i], labels[i]));
				}
				return new MemSourceBatchOp(rows, new String[] {"vec", "label"}).getOutputTable();
			} catch (Exception ex) {
				ex.printStackTrace();
				throw new RuntimeException(ex.getMessage());
			}
		}

		private int getInteger(byte[] bytes) {
			return ((bytes[0] & 0xFF) << 24) + ((bytes[1] & 0xFF) << 16) +
				((bytes[2] & 0xFF) << 8) + (bytes[3] & 0xFF);
		}

		private Integer[] getLabels() throws IOException {
			BufferedInputStream bis = new BufferedInputStream(
				new GZIPInputStream(new FileInputStream(this.labelGzFile)));
			byte[] bytes = new byte[4];
			bis.read(bytes, 0, 4);
			int magic_number = getInteger(bytes);
			bis.read(bytes, 0, 4);
			int record_number = getInteger(bytes);

			Integer[] labels = new Integer[record_number];
			for (int i = 0; i < record_number; i++) {
				labels[i] = bis.read();
			}

			bis.close();
			return labels;
		}

		private String[] getImages() throws IOException {
			BufferedInputStream bis = new BufferedInputStream(
				new GZIPInputStream(new FileInputStream(this.imageGzFile)));
			byte[] bytes = new byte[4];
			bis.read(bytes, 0, 4);
			int magic_number = getInteger(bytes);
			bis.read(bytes, 0, 4);
			int record_number = getInteger(bytes);
			bis.read(bytes, 0, 4);
			int xPixels = getInteger(bytes);
			bis.read(bytes, 0, 4);
			int yPixels = getInteger(bytes);

			int nPixels = xPixels * yPixels;
			String[] images = new String[record_number];

			if (isSparse) {
				TreeMap <Integer, Double> pixels = new TreeMap <>();
				int val;
				for (int i = 0; i < record_number; i++) {
					pixels.clear();
					for (int j = 0; j < nPixels; j++) {
						val = bis.read();
						if (0 != val) {
							pixels.put(j, (double) val);
						}
					}
					images[i] = new SparseVector(nPixels, pixels).toString();
				}
			} else {
				double[] image = new double[nPixels];
				for (int i = 0; i < record_number; i++) {
					for (int j = 0; j < nPixels; j++) {
						image[j] = bis.read();
					}
					images[i] = new DenseVector(image).toString();
				}
			}

			bis.close();
			return images;
		}

	}
}
