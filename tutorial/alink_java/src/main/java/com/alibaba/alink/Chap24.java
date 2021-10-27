package com.alibaba.alink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp;
import com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TsvSourceBatchOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TsvSourceStreamOp;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.Lookup;
import com.alibaba.alink.pipeline.recommendation.AlsRateRecommender;
import com.alibaba.alink.pipeline.recommendation.ItemCfItemsPerUserRecommender;
import com.alibaba.alink.pipeline.recommendation.ItemCfSimilarItemsRecommender;
import com.alibaba.alink.pipeline.recommendation.UserCfSimilarUsersRecommender;
import com.alibaba.alink.pipeline.recommendation.UserCfUsersPerItemRecommender;

import java.io.File;
import java.util.List;
import java.util.Map;

public class Chap24 {

	static final String DATA_DIR = Utils.ROOT_DIR + "movielens" + File.separator + "ml-100k" + File.separator;

	static final String RATING_FILE = "u.data";
	static final String USER_FILE = "u.user";
	static final String ITEM_FILE = "u.item";
	static final String RATING_TRAIN_FILE = "ua.base";
	static final String RATING_TEST_FILE = "ua.test";

	static final String USER_COL = "user_id";
	static final String ITEM_COL = "item_id";
	static final String RATING_COL = "rating";
	static final String RECOMM_COL = "recomm";

	static final String ALS_MODEL_FILE = "als_model.ak";
	static final String ITEMCF_MODEL_FILE = "itemcf_model.ak";
	static final String USERCF_MODEL_FILE = "usercf_model.ak";

	static final String RATING_SCHEMA_STRING
		= "user_id long, item_id long, rating float, ts long";

	static final String USER_SCHEMA_STRING
		= "user_id long, age int, gender string, occupation string, zip_code string";

	static final String ITEM_SCHEMA_STRING = "item_id long, title string, "
		+ "release_date string, video_release_date string, imdb_url string, "
		+ "unknown int, action int, adventure int, animation int, "
		+ "children int, comedy int, crime int, documentary int, drama int, "
		+ "fantasy int, film_noir int, horror int, musical int, mystery int, "
		+ "romance int, sci_fi int, thriller int, war int, western int";

	static TsvSourceBatchOp getSourceRatings() {
		return new TsvSourceBatchOp()
			.setFilePath(DATA_DIR + RATING_FILE)
			.setSchemaStr(RATING_SCHEMA_STRING);
	}

	static TsvSourceStreamOp getStreamSourceRatings() {
		return new TsvSourceStreamOp()
			.setFilePath(DATA_DIR + RATING_FILE)
			.setSchemaStr(RATING_SCHEMA_STRING);
	}

	static CsvSourceBatchOp getSourceUsers() {
		return new CsvSourceBatchOp()
			.setFieldDelimiter("|")
			.setFilePath(DATA_DIR + USER_FILE)
			.setSchemaStr(USER_SCHEMA_STRING);
	}

	static CsvSourceBatchOp getSourceItems() {
		return new CsvSourceBatchOp()
			.setFieldDelimiter("|")
			.setFilePath(DATA_DIR + ITEM_FILE)
			.setSchemaStr(ITEM_SCHEMA_STRING);
	}

	static CsvSourceStreamOp getStreamSourceItems() {
		return new CsvSourceStreamOp()
			.setFieldDelimiter("|")
			.setFilePath(DATA_DIR + ITEM_FILE)
			.setSchemaStr(ITEM_SCHEMA_STRING);
	}

	public static void main(String[] args) throws Exception {

		c_4();

		c_5();

		c_6();

		c_7();

		c_8();

	}

	static void c_4() throws Exception {

		TsvSourceBatchOp train_set = new TsvSourceBatchOp()
			.setFilePath(DATA_DIR + RATING_TRAIN_FILE)
			.setSchemaStr(RATING_SCHEMA_STRING);

		TsvSourceBatchOp test_set = new TsvSourceBatchOp()
			.setFilePath(DATA_DIR + RATING_TEST_FILE)
			.setSchemaStr(RATING_SCHEMA_STRING);

		if (!new File(DATA_DIR + ALS_MODEL_FILE).exists()) {

			train_set
				.link(
					new AlsTrainBatchOp()
						.setUserCol(USER_COL)
						.setItemCol(ITEM_COL)
						.setRateCol(RATING_COL)
						.setLambda(0.1)
						.setRank(10)
						.setNumIter(10)
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + ALS_MODEL_FILE)
				);
			BatchOperator.execute();

		}

		new PipelineModel
			(
				new AlsRateRecommender()
					.setUserCol(USER_COL)
					.setItemCol(ITEM_COL)
					.setRecommCol(RECOMM_COL)
					.setModelData(
						new AkSourceBatchOp()
							.setFilePath(DATA_DIR + ALS_MODEL_FILE)
					),
				new Lookup()
					.setSelectedCols(ITEM_COL)
					.setOutputCols("item_name")
					.setModelData(getSourceItems())
					.setMapKeyCols("item_id")
					.setMapValueCols("title")
			)
			.transform(
				test_set.filter("user_id=1")
			)
			.select("user_id, rating, recomm, item_name")
			.orderBy("rating, recomm", 1000)
			.lazyPrint(-1);

		BatchOperator.execute();

		new AlsRateRecommender()
			.setUserCol(USER_COL)
			.setItemCol(ITEM_COL)
			.setRecommCol(RECOMM_COL)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ALS_MODEL_FILE)
			)
			.transform(test_set)
			.link(
				new EvalRegressionBatchOp()
					.setLabelCol(RATING_COL)
					.setPredictionCol(RECOMM_COL)
					.lazyPrintMetrics()
			);
		BatchOperator.execute();

	}

	static void c_5() throws Exception {

		if (!new File(DATA_DIR + ITEMCF_MODEL_FILE).exists()) {

			getSourceRatings()
				.link(
					new ItemCfTrainBatchOp()
						.setUserCol(USER_COL)
						.setItemCol(ITEM_COL)
						.setRateCol(RATING_COL)
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
				);
			BatchOperator.execute();

		}

		MemSourceBatchOp test_data = new MemSourceBatchOp(new Long[] {1L}, "user_id");

		new ItemCfItemsPerUserRecommender()
			.setUserCol(USER_COL)
			.setRecommCol(RECOMM_COL)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
			)
			.transform(test_data)
			.print();

		LocalPredictor recomm_predictor = new ItemCfItemsPerUserRecommender()
			.setUserCol(USER_COL)
			.setRecommCol(RECOMM_COL)
			.setK(20)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
			)
			.collectLocalPredictor("user_id long");

		System.out.println(recomm_predictor.getOutputSchema());

		LocalPredictor kv_predictor = new Lookup()
			.setSelectedCols(ITEM_COL)
			.setOutputCols("item_name")
			.setModelData(getSourceItems())
			.setMapKeyCols("item_id")
			.setMapValueCols("title")
			.collectLocalPredictor("item_id long");

		System.out.println(kv_predictor.getOutputSchema());

		String recommResultStr = (String) recomm_predictor.map(Row.of(1L)).getField(1);

		System.out.println(recommResultStr);

		List <Long> recomm_ids = JsonConverter
			.fromJson(
				JsonConverter
					. <Map <String, String>>fromJson(
						recommResultStr,
						Map.class
					)
					.get("item_id"),
				new TypeReference <List <Long>>() {}.getType()
			);

		for (Long id : recomm_ids) {
			System.out.println(kv_predictor.map(Row.of(id)));
		}

		new Lookup()
			.setSelectedCols(ITEM_COL)
			.setOutputCols("item_name")
			.setModelData(getSourceItems())
			.setMapKeyCols("item_id")
			.setMapValueCols("title")
			.transform(
				getSourceRatings().filter("user_id=1 AND rating>4")
			)
			.select("item_name")
			.orderBy("item_name", 1000)
			.lazyPrint(-1);

		LocalPredictor recomm_predictor_2 = new ItemCfItemsPerUserRecommender()
			.setUserCol(USER_COL)
			.setRecommCol(RECOMM_COL)
			.setK(20)
			.setExcludeKnown(true)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
			)
			.collectLocalPredictor("user_id long");

		recommResultStr = (String) recomm_predictor_2.map(Row.of(1L)).getField(1);

		System.out.println(recommResultStr);

		recomm_ids = JsonConverter
			.fromJson(
				JsonConverter
					. <Map <String, String>>fromJson(
						recommResultStr,
						Map.class
					)
					.get("item_id"),
				new TypeReference <List <Long>>() {}.getType()
			);

		for (Long id : recomm_ids) {
			System.out.println(kv_predictor.map(Row.of(id)));
		}

	}

	static void c_6() throws Exception {
		MemSourceBatchOp test_data = new MemSourceBatchOp(new Long[] {50L}, ITEM_COL);

		new ItemCfSimilarItemsRecommender()
			.setItemCol(ITEM_COL)
			.setRecommCol(RECOMM_COL)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
			)
			.transform(test_data)
			.print();

		LocalPredictor recomm_predictor = new ItemCfSimilarItemsRecommender()
			.setItemCol(ITEM_COL)
			.setRecommCol(RECOMM_COL)
			.setK(10)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + ITEMCF_MODEL_FILE)
			)
			.collectLocalPredictor("item_id long");

		LocalPredictor kv_predictor = new Lookup()
			.setSelectedCols(ITEM_COL)
			.setOutputCols("item_name")
			.setModelData(getSourceItems())
			.setMapKeyCols("item_id")
			.setMapValueCols("title")
			.collectLocalPredictor("item_id long");

		String recommResultStr = (String) recomm_predictor.map(Row.of(50L)).getField(1);

		List <Long> recomm_ids = JsonConverter
			.fromJson(
				JsonConverter
					. <Map <String, String>>fromJson(
						recommResultStr,
						Map.class
					)
					.get("item_id"),
				new TypeReference <List <Long>>() {}.getType()
			);

		for (Long id : recomm_ids) {
			System.out.println(kv_predictor.map(Row.of(id)));
		}

	}

	static void c_7() throws Exception {

		if (!new File(DATA_DIR + USERCF_MODEL_FILE).exists()) {

			getSourceRatings()
				.link(
					new UserCfTrainBatchOp()
						.setUserCol(USER_COL)
						.setItemCol(ITEM_COL)
						.setRateCol(RATING_COL)
				)
				.link(
					new AkSinkBatchOp()
						.setFilePath(DATA_DIR + USERCF_MODEL_FILE)
				);
			BatchOperator.execute();

		}

		MemSourceBatchOp test_data = new MemSourceBatchOp(new Long[] {50L}, ITEM_COL);

		new UserCfUsersPerItemRecommender()
			.setItemCol(ITEM_COL)
			.setRecommCol(RECOMM_COL)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + USERCF_MODEL_FILE)
			)
			.transform(test_data)
			.print();

		getSourceRatings()
			.filter("user_id IN (276,429,222,864,194,650,896,303,749,301) AND item_id=50")
			.print();

		new UserCfUsersPerItemRecommender()
			.setItemCol(ITEM_COL)
			.setRecommCol(RECOMM_COL)
			.setExcludeKnown(true)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + USERCF_MODEL_FILE)
			)
			.transform(test_data)
			.print();

	}

	static void c_8() throws Exception {
		MemSourceBatchOp test_data = new MemSourceBatchOp(new Long[] {1L}, USER_COL);

		new UserCfSimilarUsersRecommender()
			.setUserCol(USER_COL)
			.setRecommCol(RECOMM_COL)
			.setModelData(
				new AkSourceBatchOp()
					.setFilePath(DATA_DIR + USERCF_MODEL_FILE)
			)
			.transform(test_data)
			.print();

		getSourceUsers()
			.filter("user_id IN (1, 916,864,268,92,435,457,738,429,303,276)")
			.print();
	}

}