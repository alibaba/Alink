package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType;
import com.alibaba.alink.pipeline.clustering.BisectingKMeans;
import com.alibaba.alink.pipeline.clustering.GaussianMixture;
import com.alibaba.alink.pipeline.clustering.GeoKMeans;
import com.alibaba.alink.pipeline.clustering.KMeans;

import java.io.File;

public class Chap17 {

	static final String DATA_DIR = Utils.ROOT_DIR + "iris" + File.separator;

	static final String ORIGIN_FILE = "iris.data";
	static final String VECTOR_FILE = "iris_vec.ak";

	static final String SCHEMA_STRING
		= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	static final String[] FEATURE_COL_NAMES
		= new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width"};

	static final String LABEL_COL_NAME = "category";
	static final String VECTOR_COL_NAME = "vec";
	static final String PREDICTION_COL_NAME = "cluster_id";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_2_2();

		c_3_2();

		c_4();

		c_5();

	}

	static void c_2_2() throws Exception {
		if (!new File(DATA_DIR + VECTOR_FILE).exists()) {
			new CsvSourceBatchOp()
				.setFilePath(DATA_DIR + ORIGIN_FILE)
				.setSchemaStr(SCHEMA_STRING)
				.link(
					new VectorAssemblerBatchOp()
						.setSelectedCols(FEATURE_COL_NAMES)
						.setOutputCol(VECTOR_COL_NAME)
						.setReservedCols(LABEL_COL_NAME)
				)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + VECTOR_FILE)
				);
			BatchOperator.execute();
		}

		AkSourceBatchOp source = new AkSourceBatchOp().setFilePath(DATA_DIR + VECTOR_FILE);

		source.lazyPrint(5);

		KMeansTrainBatchOp kmeans_model = new KMeansTrainBatchOp()
			.setK(2)
			.setVectorCol(VECTOR_COL_NAME);

		KMeansPredictBatchOp kmeans_pred = new KMeansPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME);

		source.link(kmeans_model);
		kmeans_pred.linkFrom(kmeans_model, source);

		kmeans_model.lazyPrintModelInfo();

		kmeans_pred.lazyPrint(5);

		kmeans_pred
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics("KMeans EUCLIDEAN")
			);

		kmeans_pred
			.orderBy(PREDICTION_COL_NAME + ", " + LABEL_COL_NAME, 200, false)
			.lazyPrint(-1, "all data");

		BatchOperator.execute();

		new KMeans()
			.setK(2)
			.setDistanceType(DistanceType.COSINE)
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintModelInfo()
			.fit(source)
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("KMeans COSINE")
			);
		BatchOperator.execute();
	}

	static void c_3_2() throws Exception {

		AkSourceBatchOp source = new AkSourceBatchOp().setFilePath(DATA_DIR + VECTOR_FILE);

		new GaussianMixture()
			.setK(2)
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintModelInfo()
			.fit(source)
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("GaussianMixture 2")
			);
		BatchOperator.execute();

	}

	static void c_4() throws Exception {
		AkSourceBatchOp source = new AkSourceBatchOp().setFilePath(DATA_DIR + VECTOR_FILE);

		new BisectingKMeans()
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintModelInfo("BiSecting KMeans EUCLIDEAN")
			.fit(source)
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("Bisecting KMeans EUCLIDEAN")
			);
		BatchOperator.execute();

		new BisectingKMeans()
			.setDistanceType(DistanceType.COSINE)
			.setK(3)
			.setVectorCol(VECTOR_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.enableLazyPrintModelInfo("BiSecting KMeans COSINE")
			.fit(source)
			.transform(source)
			.link(
				new EvalClusterBatchOp()
					.setDistanceType("COSINE")
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("Bisecting KMeans COSINE")
			);
		BatchOperator.execute();

	}

	static void c_5() throws Exception {
		BatchOperator.setParallelism(1);

		MemSourceBatchOp source = new MemSourceBatchOp(ROWS_DATA,
			new String[] {"State", "Region", "Division", "longitude", "latitude"}
		);

		source.lazyPrint(5);

		source.select("Region").distinct().lazyPrint(-1);

		source.select("Division").distinct().lazyPrint(-1);

		source
			.groupBy("Region, Division", "Region, Division, COUNT(*) AS numStates")
			.orderBy("Region, Division", 100)
			.lazyPrint(-1);

		for (int nClusters : new int[] {2, 4}) {
			BatchOperator <?> pred = new GeoKMeans()
				.setLongitudeCol("longitude")
				.setLatitudeCol("latitude")
				.setPredictionCol(PREDICTION_COL_NAME)
				.setK(nClusters)
				.fit(source)
				.transform(source);

			pred.link(
				new EvalClusterBatchOp()
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol("Region")
					.lazyPrintMetrics(nClusters + " with Region")
			);
			pred.link(
				new EvalClusterBatchOp()
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol("Division")
					.lazyPrintMetrics(nClusters + " with Division")
			);
			BatchOperator.execute();
		}

	}

	private static final Row[] ROWS_DATA = new Row[] {
		Row.of("Alabama", "South", "East South Central", -86.7509, 32.5901),
		Row.of("Alaska", "West", "Pacific", -127.25, 49.25),
		Row.of("Arizona", "West", "Mountain", -111.625, 34.2192),
		Row.of("Arkansas", "South", "West South Central", -92.2992, 34.7336),
		Row.of("California", "West", "Pacific", -119.773, 36.5341),
		Row.of("Colorado", "West", "Mountain", -105.513, 38.6777),
		Row.of("Connecticut", "Northeast", "New England", -72.3573, 41.5928),
		Row.of("Delaware", "South", "South Atlantic", -74.9841, 38.6777),
		Row.of("Florida", "South", "South Atlantic", -81.685, 27.8744),
		Row.of("Georgia", "South", "South Atlantic", -83.3736, 32.3329),
		Row.of("Hawaii", "West", "Pacific", -126.25, 31.75),
		Row.of("Idaho", "West", "Mountain", -113.93, 43.5648),
		Row.of("Illinois", "North Central", "East North Central", -89.3776, 40.0495),
		Row.of("Indiana", "North Central", "East North Central", -86.0808, 40.0495),
		Row.of("Iowa", "North Central", "West North Central", -93.3714, 41.9358),
		Row.of("Kansas", "North Central", "West North Central", -98.1156, 38.4204),
		Row.of("Kentucky", "South", "East South Central", -84.7674, 37.3915),
		Row.of("Louisiana", "South", "West South Central", -92.2724, 30.6181),
		Row.of("Maine", "Northeast", "New England", -68.9801, 45.6226),
		Row.of("Maryland", "South", "South Atlantic", -76.6459, 39.2778),
		Row.of("Massachusetts", "Northeast", "New England", -71.58, 42.3645),
		Row.of("Michigan", "North Central", "East North Central", -84.687, 43.1361),
		Row.of("Minnesota", "North Central", "West North Central", -94.6043, 46.3943),
		Row.of("Mississippi", "South", "East South Central", -89.8065, 32.6758),
		Row.of("Missouri", "North Central", "West North Central", -92.5137, 38.3347),
		Row.of("Montana", "West", "Mountain", -109.32, 46.823),
		Row.of("Nebraska", "North Central", "West North Central", -99.5898, 41.3356),
		Row.of("Nevada", "West", "Mountain", -116.851, 39.1063),
		Row.of("New Hampshire", "Northeast", "New England", -71.3924, 43.3934),
		Row.of("New Jersey", "Northeast", "Middle Atlantic", -74.2336, 39.9637),
		Row.of("New Mexico", "West", "Mountain", -105.942, 34.4764),
		Row.of("New York", "Northeast", "Middle Atlantic", -75.1449, 43.1361),
		Row.of("North Carolina", "South", "South Atlantic", -78.4686, 35.4195),
		Row.of("North Dakota", "North Central", "West North Central", -100.099, 47.2517),
		Row.of("Ohio", "North Central", "East North Central", -82.5963, 40.221),
		Row.of("Oklahoma", "South", "West South Central", -97.1239, 35.5053),
		Row.of("Oregon", "West", "Pacific", -120.068, 43.9078),
		Row.of("Pennsylvania", "Northeast", "Middle Atlantic", -77.45, 40.9069),
		Row.of("Rhode Island", "Northeast", "New England", -71.1244, 41.5928),
		Row.of("South Carolina", "South", "South Atlantic", -80.5056, 33.619),
		Row.of("South Dakota", "North Central", "West North Central", -99.7238, 44.3365),
		Row.of("Tennessee", "South", "East South Central", -86.456, 35.6767),
		Row.of("Texas", "South", "West South Central", -98.7857, 31.3897),
		Row.of("Utah", "West", "Mountain", -111.33, 39.1063),
		Row.of("Vermont", "Northeast", "New England", -72.545, 44.2508),
		Row.of("Virginia", "South", "South Atlantic", -78.2005, 37.563),
		Row.of("Washington", "West", "Pacific", -119.746, 47.4231),
		Row.of("West Virginia", "South", "South Atlantic", -80.6665, 38.4204),
		Row.of("Wisconsin", "North Central", "East North Central", -89.9941, 44.5937),
		Row.of("Wyoming", "West", "Mountain", -107.256, 43.0504)
	};

}
