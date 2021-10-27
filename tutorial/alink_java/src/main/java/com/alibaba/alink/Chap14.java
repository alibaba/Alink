package com.alibaba.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TextSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlModelFilterStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.feature.FeatureHasher;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;

public class Chap14 {

	private static final String DATA_DIR = Utils.ROOT_DIR + "ctr_avazu" + File.separator;

	static final String SCHEMA_STRING
		= "id string, click string, dt string, C1 string, banner_pos int, site_id string, site_domain string, "
		+ "site_category string, app_id string, app_domain string, app_category string, device_id string, "
		+ "device_ip string, device_model string, device_type string, device_conn_type string, C14 int, C15 int, "
		+ "C16 int, C17 int, C18 int, C19 int, C20 int, C21 int";

	static final String[] CATEGORY_COL_NAMES = new String[] {
		"C1", "banner_pos", "site_category", "app_domain",
		"app_category", "device_type", "device_conn_type",
		"site_id", "site_domain", "device_id", "device_model"};

	static final String[] NUMERICAL_COL_NAMES = new String[] {
		"C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21"};

	static final String FEATURE_MODEL_FILE = "feature_model.ak";
	static final String INIT_MODEL_FILE = "init_model.ak";

	static final String LABEL_COL_NAME = "click";
	static final String VEC_COL_NAME = "vec";
	static final String PREDICTION_COL_NAME = "pred";
	static final String PRED_DETAIL_COL_NAME = "pred_info";

	static final int NUM_HASH_FEATURES = 30000;

	public static void main(String[] args) throws Exception {

		c_2();

		c_3();

		c_4();

		c_5();

		c_6();

	}

	static void c_2() throws Exception {

		new TextSourceBatchOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-small.csv")
			.firstN(10)
			.print();

		CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-small.csv")
			.setSchemaStr(SCHEMA_STRING);

		trainBatchData.firstN(10).print();

	}

	static void c_3() throws Exception {
		CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-small.csv")
			.setSchemaStr(SCHEMA_STRING);

		// setup feature enginerring pipeline
		Pipeline feature_pipeline = new Pipeline()
			.add(
				new StandardScaler()
					.setSelectedCols(NUMERICAL_COL_NAMES)
			)
			.add(
				new FeatureHasher()
					.setSelectedCols(ArrayUtils.addAll(CATEGORY_COL_NAMES, NUMERICAL_COL_NAMES))
					.setCategoricalCols(CATEGORY_COL_NAMES)
					.setOutputCol(VEC_COL_NAME)
					.setNumFeatures(NUM_HASH_FEATURES)
			);

		if (!new File(DATA_DIR + FEATURE_MODEL_FILE).exists()) {
			// fit and save feature pipeline model
			feature_pipeline
				.fit(trainBatchData)
				.save(DATA_DIR + FEATURE_MODEL_FILE);
			BatchOperator.execute();
		}
	}

	static void c_4() throws Exception {
		// load pipeline model
		PipelineModel feature_pipelineModel = PipelineModel.load(DATA_DIR + FEATURE_MODEL_FILE);

		// prepare stream train data
		CsvSourceStreamOp data = new CsvSourceStreamOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-ctr-train-8M.csv")
			.setSchemaStr(SCHEMA_STRING);

		if (!new File(DATA_DIR + INIT_MODEL_FILE).exists()) {
			CsvSourceBatchOp trainBatchData = new CsvSourceBatchOp()
				.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-small.csv")
				.setSchemaStr(SCHEMA_STRING);

			// train initial batch model
			LogisticRegressionTrainBatchOp lr = new LogisticRegressionTrainBatchOp()
				.setVectorCol(VEC_COL_NAME)
				.setLabelCol(LABEL_COL_NAME)
				.setWithIntercept(true)
				.setMaxIter(10);

			feature_pipelineModel
				.transform(trainBatchData)
				.link(lr)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + INIT_MODEL_FILE)
				);
			BatchOperator.execute();
		}

	}

	static void c_5() throws Exception {

		// load pipeline model
		PipelineModel feature_pipelineModel = PipelineModel.load(DATA_DIR + FEATURE_MODEL_FILE);

		BatchOperator initModel = new AkSourceBatchOp().setFilePath(DATA_DIR + INIT_MODEL_FILE);

		// prepare stream train data
		CsvSourceStreamOp data = new CsvSourceStreamOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-ctr-train-8M.csv")
			.setSchemaStr(SCHEMA_STRING)
			.setIgnoreFirstLine(true);

		// split stream to train and eval data
		SplitStreamOp spliter = new SplitStreamOp().setFraction(0.5).linkFrom(data);
		StreamOperator train_stream_data = feature_pipelineModel.transform(spliter);
		StreamOperator test_stream_data = feature_pipelineModel.transform(spliter.getSideOutput(0));

		// ftrl train
		FtrlTrainStreamOp model = new FtrlTrainStreamOp(initModel)
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setWithIntercept(true)
			.setAlpha(0.1)
			.setBeta(0.1)
			.setL1(0.01)
			.setL2(0.01)
			.setTimeInterval(10)
			.setVectorSize(NUM_HASH_FEATURES)
			.linkFrom(train_stream_data);

		// ftrl predict
		FtrlPredictStreamOp predResult = new FtrlPredictStreamOp(initModel)
			.setVectorCol(VEC_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setReservedCols(new String[] {LABEL_COL_NAME})
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.linkFrom(model, test_stream_data);

		predResult
			.sample(0.0001)
			.select("'Pred Sample' AS out_type, *")
			.print();

		// ftrl eval
		predResult
			.link(
				new EvalBinaryClassStreamOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.setTimeInterval(10)
			)
			.link(
				new JsonValueStreamOp()
					.setSelectedCol("Data")
					.setReservedCols(new String[] {"Statistics"})
					.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
					.setJsonPath(new String[] {"$.Accuracy", "$.AUC", "$.ConfusionMatrix"})
			)
			.select("'Eval Metric' AS out_type, *")
			.print();

		StreamOperator.execute();

	}

	static void c_6() throws Exception {

		// prepare stream train data
		CsvSourceStreamOp data = new CsvSourceStreamOp()
			.setFilePath("http://alink-release.oss-cn-beijing.aliyuncs.com/data-files/avazu-ctr-train-8M.csv")
			.setSchemaStr(SCHEMA_STRING)
			.setIgnoreFirstLine(true);

		// load pipeline model
		PipelineModel feature_pipelineModel = PipelineModel.load(DATA_DIR + FEATURE_MODEL_FILE);

		// split stream to train and eval data
		SplitStreamOp spliter = new SplitStreamOp().setFraction(0.5).linkFrom(data);
		StreamOperator <?> train_stream_data = feature_pipelineModel.transform(spliter);
		StreamOperator <?> test_stream_data = feature_pipelineModel.transform(spliter.getSideOutput(0));

		AkSourceBatchOp initModel = new AkSourceBatchOp().setFilePath(DATA_DIR + INIT_MODEL_FILE);

		// ftrl train
		FtrlTrainStreamOp model = new FtrlTrainStreamOp(initModel)
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setWithIntercept(true)
			.setAlpha(0.1)
			.setBeta(0.1)
			.setL1(0.01)
			.setL2(0.01)
			.setTimeInterval(10)
			.setVectorSize(NUM_HASH_FEATURES)
			.linkFrom(train_stream_data);

		// model filter
		FtrlModelFilterStreamOp model_filter = new FtrlModelFilterStreamOp()
			.setPositiveLabelValueString("1")
			.setVectorCol(VEC_COL_NAME)
			.setLabelCol(LABEL_COL_NAME)
			.setAccuracyThreshold(0.83)
			.setAucThreshold(0.71)
			.linkFrom(model, train_stream_data);

		model_filter
			.select("'Model' AS out_type, *")
			.print();

		// ftrl predict
		FtrlPredictStreamOp predResult = new FtrlPredictStreamOp(initModel)
			.setVectorCol(VEC_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setReservedCols(new String[] {LABEL_COL_NAME})
			.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
			.linkFrom(model_filter, test_stream_data);

		predResult
			.sample(0.0001)
			.select("'Pred Sample' AS out_type, *")
			.print();

		// ftrl eval
		predResult
			.link(
				new EvalBinaryClassStreamOp()
					.setPositiveLabelValueString("1")
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionDetailCol(PRED_DETAIL_COL_NAME)
					.setTimeInterval(10)
			)
			.link(
				new JsonValueStreamOp()
					.setSelectedCol("Data")
					.setReservedCols(new String[] {"Statistics"})
					.setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
					.setJsonPath(new String[] {"$.Accuracy", "$.AUC", "$.ConfusionMatrix"})
			)
			.select("'Eval Metric' AS out_type, *")
			.print();

		StreamOperator.execute();

	}

}
