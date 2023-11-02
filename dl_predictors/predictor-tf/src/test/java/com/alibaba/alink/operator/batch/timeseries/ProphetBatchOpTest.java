package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class ProphetBatchOpTest {

	@Test
	public void testModel() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of(1, new Timestamp(1000), 10.0),
				Row.of(1, new Timestamp(2000), 11.0),
				Row.of(1, new Timestamp(3000), 12.0),
				Row.of(1, new Timestamp(4000), 13.0),
				Row.of(1, new Timestamp(5000), 14.0),
				Row.of(1, new Timestamp(6000), 15.0),
				Row.of(1, new Timestamp(7000), 16.0),
				Row.of(1, new Timestamp(8000), 17.0),
				Row.of(1, new Timestamp(9000), 18.0),
				Row.of(1, new Timestamp(10000), 19.0)
			};
		String[] colNames = new String[] {"id", "ds1", "y1"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		//source.print();

		ProphetTrainBatchOp trainOp = new ProphetTrainBatchOp()
			.setTimeCol("ds1")
			.setValueCol("y1");

		source.link(trainOp);

		trainOp.lazyPrint();

		//construct times series by id.
		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ds1, y1) as data");

		ProphetPredictBatchOp predictOp = new ProphetPredictBatchOp()
			.setValueCol("data")
			.setPredictNum(4)
			.setPredictionCol("pred");

		predictOp.linkFrom(trainOp, source.link(groupData)).print();
	}

	@Test
	public void test() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", Timestamp.valueOf("2023-01-01 00:00:00"), 9.59076113897809),
				Row.of("1", Timestamp.valueOf("2023-01-02 00:00:00"), 8.51959031601596),
				Row.of("2", Timestamp.valueOf("2023-01-03 00:00:00"), 9.59076113897809),
				Row.of("1", Timestamp.valueOf("2023-01-04 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-05 00:00:00"), 8.51959031601596),
				Row.of("1", Timestamp.valueOf("2023-01-06 00:00:00"), 8.07246736935477),
				Row.of("2", Timestamp.valueOf("2023-01-07 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-08 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-09 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-10 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-11 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-12 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-13 00:00:00"), 8.18367658262066),
				Row.of("2", Timestamp.valueOf("2023-01-14 00:00:00"), 8.18367658262066),
				Row.of("1", Timestamp.valueOf("2023-01-15 00:00:00"), 7.8935720735049),
				Row.of("1", Timestamp.valueOf("2023-01-16 00:00:00"), 7.78364059622125),
				Row.of("2", Timestamp.valueOf("2023-01-17 00:00:00"), 8.07246736935477),
				Row.of("1", Timestamp.valueOf("2023-01-18 00:00:00"), 8.41405243249672),
				Row.of("1", Timestamp.valueOf("2023-01-19 00:00:00"), 8.82922635473185),
				Row.of("1", Timestamp.valueOf("2023-01-20 00:00:00"), 8.38251828808963),
				Row.of("1", Timestamp.valueOf("2023-01-21 00:00:00"), 8.06965530688617),
				Row.of("1", Timestamp.valueOf("2023-01-22 00:00:00"), 9.59076113897809),
				Row.of("1", Timestamp.valueOf("2023-01-23 00:00:00"), 8.51959031601596),
				Row.of("1", Timestamp.valueOf("2023-01-24 00:00:00"), 8.18367658262066),
				Row.of("1", Timestamp.valueOf("2023-01-25 00:00:00"), 8.07246736935477),
				Row.of("1", Timestamp.valueOf("2023-01-26 00:00:00"), 7.8935720735049),
				Row.of("1", Timestamp.valueOf("2023-01-27 00:00:00"), 7.78364059622125),
				Row.of("1", Timestamp.valueOf("2023-01-28 00:00:00"), 8.41405243249672),
				Row.of("1", Timestamp.valueOf("2023-01-29 00:00:00"), 8.82922635473185),
				Row.of("1", Timestamp.valueOf("2023-02-01 00:00:00"), 8.38251828808963),
				Row.of("1", Timestamp.valueOf("2023-02-02 00:00:00"), 8.06965530688617),
				Row.of("2", Timestamp.valueOf("2023-02-03 00:00:00"), 8.07246736935477),
				Row.of("2", Timestamp.valueOf("2023-02-04 00:00:00"), 7.8935720735049),
				Row.of("2", Timestamp.valueOf("2023-02-05 00:00:00"), 7.78364059622125),
				Row.of("2", Timestamp.valueOf("2023-02-06 00:00:00"), 8.41405243249672),
				Row.of("2", Timestamp.valueOf("2023-02-07 00:00:00"), 8.82922635473185),
				Row.of("2", Timestamp.valueOf("2023-02-08 00:00:00"), 8.38251828808963),
				Row.of("2", Timestamp.valueOf("2023-02-09 00:00:00"), 8.06965530688617)
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

	@Test
	public void testc() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1000), 10.0),
			Row.of(1, new Timestamp(2000), 11.0),
			Row.of(1, new Timestamp(3000), 12.0),
			Row.of(1, new Timestamp(4000), 13.0),
			Row.of(1, new Timestamp(5000), 14.0),
			Row.of(1, new Timestamp(6000), 15.0),
			Row.of(1, new Timestamp(7000), 16.0),
			Row.of(1, new Timestamp(8000), 17.0),
			Row.of(1, new Timestamp(9000), 18.0),
			Row.of(1, new Timestamp(10000), 19.0)
		);

		MemSourceBatchOp source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts", "val"});

		source.link(
			new GroupByBatchOp()
				.setGroupByPredicate("id")
				.setSelectClause("mtable_agg(ts, val) as data")
		).link(new ProphetBatchOp()
			.setValueCol("data")
			.setPredictNum(4)
			.setPredictionCol("pred")
		).print();
	}

}