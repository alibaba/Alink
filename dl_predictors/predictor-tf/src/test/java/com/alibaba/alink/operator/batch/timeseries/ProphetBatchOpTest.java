package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.dataproc.TensorToVectorBatchOp;
import com.alibaba.alink.operator.batch.dataproc.VectorToTensorBatchOp;
import com.alibaba.alink.operator.batch.dataproc.format.TripleToColumnsBatchOp;
import com.alibaba.alink.operator.batch.dataproc.format.VectorToTripleBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.params.timeseries.ProphetParams.SeasonalityMode;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public class ProphetBatchOpTest {

	@Test
	public void testModel() throws Exception {
		//AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), 8.51959031601596),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 14, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 15, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 16, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 11, 17, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 18, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 19, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 11, 20, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 11, 21, 0, 0, 0, 0), 8.06965530688617),
				Row.of("1", new Timestamp(117, 11, 22, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 23, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 24, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 25, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 26, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 27, 0, 0, 0, 0), 7.78364059622125),
				Row.of("1", new Timestamp(117, 11, 28, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 29, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 12, 1, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 12, 2, 0, 0, 0, 0), 8.06965530688617),
				Row.of("2", new Timestamp(117, 12, 3, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 12, 4, 0, 0, 0, 0), 7.8935720735049),
				Row.of("2", new Timestamp(117, 12, 5, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 12, 6, 0, 0, 0, 0), 8.41405243249672),
				Row.of("2", new Timestamp(117, 12, 7, 0, 0, 0, 0), 8.82922635473185),
				Row.of("2", new Timestamp(117, 12, 8, 0, 0, 0, 0), 8.38251828808963),
				Row.of("2", new Timestamp(117, 12, 9, 0, 0, 0, 0), 8.06965530688617)
			};
		String[] colNames = new String[] {"id", "ts", "val"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		ProphetTrainBatchOp model = new ProphetTrainBatchOp()
			.setTimeCol("ts")
			.setValueCol("val");

		source.link(model).print();

		//construct times series by id.
		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, val) as data");

		ProphetPredictBatchOp prophetPredict = new ProphetPredictBatchOp()
			.setValueCol("data")
			.setPredictNum(4)
			.setPredictionCol("pred");

		prophetPredict.linkFrom(model, source.link(groupData)).print();
	}

	@Test
	public void test() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), 8.51959031601596),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 14, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 15, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 16, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 11, 17, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 18, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 19, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 11, 20, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 11, 21, 0, 0, 0, 0), 8.06965530688617),
				Row.of("1", new Timestamp(117, 11, 22, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 23, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 24, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 25, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 26, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 27, 0, 0, 0, 0), 7.78364059622125),
				Row.of("1", new Timestamp(117, 11, 28, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 29, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 12, 1, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 12, 2, 0, 0, 0, 0), 8.06965530688617),
				Row.of("2", new Timestamp(117, 12, 3, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 12, 4, 0, 0, 0, 0), 7.8935720735049),
				Row.of("2", new Timestamp(117, 12, 5, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 12, 6, 0, 0, 0, 0), 8.41405243249672),
				Row.of("2", new Timestamp(117, 12, 7, 0, 0, 0, 0), 8.82922635473185),
				Row.of("2", new Timestamp(117, 12, 8, 0, 0, 0, 0), 8.38251828808963),
				Row.of("2", new Timestamp(117, 12, 9, 0, 0, 0, 0), 8.06965530688617)
			};
		String[] colNames = new String[] {"id", "ts", "val"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		//construct times series by id.
		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, val) as data");

		ProphetBatchOp prophetPredict = new ProphetBatchOp()
			.setValueCol("data")
			.setPredictNum(4)
			.setPredictionCol("pred");

		prophetPredict.linkFrom(source.link(groupData)).print();
	}

	@Ignore
	@Test
	public void test2() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Row[] rowsData =
			new Row[] {
				Row.of("20210501", 0.1, "a1"),
				Row.of("20210502", 0.2, "a1"),
				Row.of("20210503", 0.3, "a1"),
				Row.of("20210504", 0.4, "a1"),
				Row.of("20210505", 0.5, "a1"),
				Row.of("20210506", 0.6, "a1"),
				Row.of("20210507", 0.7, "a1"),
				Row.of("20210508", 0.8, "a1"),
				Row.of("20210509", 0.9, "a1"),
			};
		String[] colNames = new String[] {"ds", "f1", "f2"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		source.print();

		BatchOperator <?> prophetOp = source.select("to_timestamp(ds, 'yyyyMMdd') as ds, f1 as val, f2 as id")
			.link(
				new GroupByBatchOp()
					.setGroupByPredicate("id")
					.setSelectClause("id, mtable_agg(ds, val) as data")
			)
			.link(
				new ProphetBatchOp()
					.setValueCol("data")
					.setPredictionCol("pred")
					.setPredictionDetailCol("pred_detail")
					.setPredictNum(12)
					//.setUncertaintySamples(1000)
					.setHolidays("playoff:2021-05-03,2021-01-03 superbowl:2021-02-07,2021-11-02")
					//.setGrowth("logistic")
					.setGrowth("linear")
					.setCap(6.0)
					.setFloor(1.0)
					.setChangePoints("2021-05-02,2021-05-07")
					.setChangePointRange(0.5)
					.setChangePointPriorScale(0.05)
					.setNChangePoint(24)
					.setHolidaysPriorScale(0.05)
					//.setDailySeasonality("true")
					//.setWeeklySeasonality("true")
					//.setYearlySeasonality("true")
					//.setDailySeasonality("false")
					//.setWeeklySeasonality("false")
					//.setYearlySeasonality("false")
					.setDailySeasonality("auto")
					.setWeeklySeasonality("auto")
					.setYearlySeasonality("auto")
					.setIntervalWidth(0.6)
					.setSeasonalityMode("ADDITIVE")
					//.setSeasonalityMode("MULTIPLICATIVE")
					.setSeasonalityPriorScale(0.05)
					.setIncludeHistory(false)
					.setReservedCols("id")
			)
			.link(
				new FlattenMTableBatchOp()
					.setSelectedCol("pred_detail")
					//.setSchemaStr("ds timestamp, "
					//	+ "yhat double, yhat_lower double, yhat_upper double")
					.setSchemaStr("ds timestamp, "
						+ "yhat double, yhat_lower double, yhat_upper double, "
						+ "superbowl double, superbowl_upper double, superbowl_lower double,"
						+ "playoff double, playoff_upper double, playoff_lower double")
					.setReservedCols("id")
			)
			.print();
	}

	//@Test
	//public void test3() throws Exception {
	//	AlinkGlobalConfiguration.setPrintProcessInfo(true);
	//
	//	BatchOperator <?> source = new CsvSourceBatchOp()
	//		.setFilePath("/Users/ning.cain/data/prophet_testdata.csv")
	//		.setSchemaStr("dt string,money double,pid string")
	//		.setIgnoreFirstLine(true);
	//
	//	source.select("to_timestamp(dt, 'yyyy-MM-dd') as ds, money as val, pid as id")
	//		.link(
	//			new GroupByBatchOp()
	//				.setGroupByPredicate("id")
	//				.setSelectClause("id, mtable_agg(ds, val) as data")
	//		)
	//		.link(
	//			new ProphetBatchOp()
	//				.setValueCol("data")
	//				.setPredictionCol("pred")
	//				.setPredictionDetailCol("pred_detail")
	//				.setPredictNum(12)
	//				.setUncertaintySamples(1000)
	//				.setReservedCols("id")
	//		)
	//		.link(
	//			new FlattenMTableBatchOp()
	//				.setSelectedCol("pred_detail")
	//				.setSchemaStr("ds timestamp, "
	//					+ "yhat double, yhat_lower double, yhat_upper double")
	//				.setReservedCols("id")
	//		)
	//		.print();
	//
	//}

}