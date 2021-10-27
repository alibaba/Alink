package com.alibaba.alink;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp;
import com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.BisectingKMeans;
import com.alibaba.alink.pipeline.clustering.KMeans;

import java.io.File;
import java.util.ArrayList;

public class Chap18 {

	static final String DATA_DIR = Utils.ROOT_DIR + "mnist" + File.separator;

	static final String DENSE_TRAIN_FILE = "dense_train.ak";
	static final String SPARSE_TRAIN_FILE = "sparse_train.ak";

	static final String INIT_MODEL_FILE = "init_model.ak";
	static final String TEMP_STREAM_FILE = "temp_stream.ak";

	static final String VECTOR_COL_NAME = "vec";
	static final String LABEL_COL_NAME = "label";
	static final String PREDICTION_COL_NAME = "cluster_id";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(4);

		c_1();

		c_2();

		c_3();

	}

	static void c_1() throws Exception {

		AkSourceBatchOp dense_source = new AkSourceBatchOp().setFilePath(DATA_DIR + DENSE_TRAIN_FILE);
		AkSourceBatchOp sparse_source = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		Stopwatch sw = new Stopwatch();

		ArrayList <Tuple2 <String, Pipeline>> pipelineList = new ArrayList <>();
		pipelineList.add(new Tuple2 <>("KMeans EUCLIDEAN",
			new Pipeline()
				.add(
					new KMeans()
						.setK(10)
						.setVectorCol(VECTOR_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
				)
		));
		pipelineList.add(new Tuple2 <>("KMeans COSINE",
			new Pipeline()
				.add(
					new KMeans()
						.setDistanceType(DistanceType.COSINE)
						.setK(10)
						.setVectorCol(VECTOR_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
				)
		));
		pipelineList.add(new Tuple2 <>("BisectingKMeans",
			new Pipeline()
				.add(
					new BisectingKMeans()
						.setK(10)
						.setVectorCol(VECTOR_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
				)
		));

		for (Tuple2 <String, Pipeline> pipelineTuple2 : pipelineList) {
			sw.reset();
			sw.start();
			pipelineTuple2.f1
				.fit(dense_source)
				.transform(dense_source)
				.link(
					new EvalClusterBatchOp()
						.setVectorCol(VECTOR_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.setLabelCol(LABEL_COL_NAME)
						.lazyPrintMetrics(pipelineTuple2.f0 + " DENSE")
				);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());

			sw.reset();
			sw.start();
			pipelineTuple2.f1
				.fit(sparse_source)
				.transform(sparse_source)
				.link(
					new EvalClusterBatchOp()
						.setVectorCol(VECTOR_COL_NAME)
						.setPredictionCol(PREDICTION_COL_NAME)
						.setLabelCol(LABEL_COL_NAME)
						.lazyPrintMetrics(pipelineTuple2.f0 + " SPARSE")
				);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());
		}

	}

	static void c_2() throws Exception {
		AkSourceBatchOp batch_source = new AkSourceBatchOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);
		AkSourceStreamOp stream_source = new AkSourceStreamOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);

		if (!new File(DATA_DIR + INIT_MODEL_FILE).exists()) {
			batch_source
				.sampleWithSize(100)
				.link(
					new KMeansTrainBatchOp()
						.setVectorCol(VECTOR_COL_NAME)
						.setK(10)
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + INIT_MODEL_FILE)
				);
			BatchOperator.execute();
		}

		AkSourceBatchOp init_model = new AkSourceBatchOp().setFilePath(DATA_DIR + INIT_MODEL_FILE);

		new KMeansPredictBatchOp()
			.setPredictionCol(PREDICTION_COL_NAME)
			.linkFrom(init_model, batch_source)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("Batch Prediction")
			);
		BatchOperator.execute();

		stream_source
			.link(
				new KMeansPredictStreamOp(init_model)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TEMP_STREAM_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + TEMP_STREAM_FILE)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("Stream Prediction")
			);
		BatchOperator.execute();
	}

	static void c_3() throws Exception {
		AkSourceStreamOp stream_source
			= new AkSourceStreamOp().setFilePath(DATA_DIR + SPARSE_TRAIN_FILE);

		AkSourceBatchOp init_model
			= new AkSourceBatchOp().setFilePath(DATA_DIR + INIT_MODEL_FILE);

		StreamOperator<?> stream_pred =  stream_source
			.link(
				new StreamingKMeansStreamOp(init_model)
					.setTimeInterval(1L)
					.setHalfLife(1)
					.setPredictionCol(PREDICTION_COL_NAME)
			)
			.select(PREDICTION_COL_NAME + ", " + LABEL_COL_NAME +", " + VECTOR_COL_NAME);

		stream_pred.sample(0.001).print();

		stream_pred
			.link(
				new AkSinkStreamOp()
					.setFilePath(DATA_DIR + TEMP_STREAM_FILE)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceBatchOp()
			.setFilePath(DATA_DIR + TEMP_STREAM_FILE)
			.link(
				new EvalClusterBatchOp()
					.setVectorCol(VECTOR_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.setLabelCol(LABEL_COL_NAME)
					.lazyPrintMetrics("StreamingKMeans")
			);
		BatchOperator.execute();
	}

}
