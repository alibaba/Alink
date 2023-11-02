package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class InsightTest {

	@Test
	public void testOutstandingNo1() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "1", 0));
		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(3, "3", 0));
		rows.add(Row.of(4, "4", 0));
		rows.add(Row.of(5, "5", 0));
		rows.add(Row.of(6, "6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		//source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingNo1);
		System.out.println(insight);
	}

	@Test
	public void testOutstandingNoLast() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(-10000, "1", 0));
		rows.add(Row.of(-1, "2", 0));
		rows.add(Row.of(-3, "3", 0));
		rows.add(Row.of(-4, "4", 0));
		rows.add(Row.of(-5, "5", 0));
		rows.add(Row.of(-6, "6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingLast);
		System.out.println(insight);
	}

	@Test
	public void testEveness() {
		List <Row> rows = new ArrayList <>();

		//rows.add(Row.of(17, "1", 0));
		//rows.add(Row.of(27, "2", 0));
		//rows.add(Row.of(10, "3", 0));
		//rows.add(Row.of(28, "4", 0));
		//rows.add(Row.of(18, "5", 0));

		rows.add(Row.of(10, "1", 0));
		rows.add(Row.of(11, "2", 0));
		rows.add(Row.of(9, "3", 0));
		rows.add(Row.of(10, "4", 0));
		rows.add(Row.of(9, "5", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Evenness);
		System.out.println(insight);
	}

	@Test
	public void testOutstandingTop2() {
		List <Row> rows = new ArrayList <>();

		//rows.add(Row.of(17, "1", 0));
		//rows.add(Row.of(27, "2", 0));
		//rows.add(Row.of(10, "3", 0));
		//rows.add(Row.of(28, "4", 0));
		//rows.add(Row.of(18, "5", 0));

		rows.add(Row.of(1000, "1", 0));
		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(3, "3", 0));
		rows.add(Row.of(4, "4", 0));
		rows.add(Row.of(5, "5", 0));
		rows.add(Row.of(6, "6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingTop2);
		System.out.println(insight);
	}

	@Test
	public void testAttribution() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1000, "1", 0));
		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(3, "3", 0));
		rows.add(Row.of(4, "4", 0));
		rows.add(Row.of(5, "5", 0));
		rows.add(Row.of(6, "6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingNo1);
		System.out.println(insight);
		Insight insight2 = Mining.calcInsight(source, subject, InsightType.Attribution);
		System.out.println(insight2);
	}

	@Test
	public void testOutlier() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(-10000, "1", 0));
		rows.add(Row.of(-1, "2", 0));
		rows.add(Row.of(-3, "3", 0));
		rows.add(Row.of(-4, "4", 0));
		rows.add(Row.of(-5, "5", 0));
		rows.add(Row.of(-6, "6", 0));
		rows.add(Row.of(-1, "7", 0));
		rows.add(Row.of(-3, "8", 0));
		rows.add(Row.of(-4, "9", 0));
		rows.add(Row.of(-5, "10", 0));
		rows.add(Row.of(-6, "11", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Outlier);
		System.out.println(insight);
	}

	@Test
	public void testOTrend() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "1", 0));
		rows.add(Row.of(2, "2", 0));
		rows.add(Row.of(3, "3", 0));
		rows.add(Row.of(4, "4", 0));
		rows.add(Row.of(5, "5", 0));
		rows.add(Row.of(6, "6", 0));
		rows.add(Row.of(7, "7", 0));
		rows.add(Row.of(8, "8", 0));
		rows.add(Row.of(9, "9", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Trend);
		System.out.println(insight);
	}

	@Test
	public void testSeasonality() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, "01", 0));
		rows.add(Row.of(2, "02", 0));
		rows.add(Row.of(3, "03", 0));
		rows.add(Row.of(4, "04", 0));
		rows.add(Row.of(1, "05", 0));
		rows.add(Row.of(2, "06", 0));
		rows.add(Row.of(3, "07", 0));
		rows.add(Row.of(4, "08", 0));
		rows.add(Row.of(1, "09", 0));
		rows.add(Row.of(2, "10", 0));
		rows.add(Row.of(3, "11", 0));
		rows.add(Row.of(4, "12", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Seasonality);
		System.out.println(insight);
	}

	@Test
	public void testSeasonality2() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, new Timestamp(1000), 1));
		rows.add(Row.of(2, new Timestamp(1000), 2));
		rows.add(Row.of(3, new Timestamp(1000), 3));
		rows.add(Row.of(4, new Timestamp(1000), 4));
		rows.add(Row.of(1, new Timestamp(13), 0));
		rows.add(Row.of(2, new Timestamp(13), 0));
		rows.add(Row.of(3, new Timestamp(13), 0));
		rows.add(Row.of(4, new Timestamp(13), 0));
		rows.add(Row.of(1, new Timestamp(13), 0));
		rows.add(Row.of(2, new Timestamp(13), 0));
		rows.add(Row.of(3, new Timestamp(13), 0));
		rows.add(Row.of(4, new Timestamp(13), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 INT, col1 TIMESTAMP, label INT");
		source.lazyPrint();
		//source.select("*, label + 1 as f0").lazyPrint();
		//source.select("date_format_ltz(col1) as c1").lazyPrint();
		//source
		//	.select("col1,TIMESTAMP'1970-01-01 00:00:00.012' as col2, CAST('1970-01-01 00:00:00.012' AS TIMESTAMP) as col3,label")
		//	//.lazyPrint()
		//	////.select(new String[]{"col1"})
		//	//.select("col1,col2")
		//	//.select("col1")
		//	.filter("unix_timestamp_macro(col1)=1000")
		//	//.filter("col1=CAST('1970-01-01 00:00:01' AS TIMESTAMP)")
		//	//.filter("col1=TIMESTAMP'1970-01-01 00:00:01'")
		//	.lazyPrint();

		// for groupby test.
		//source
		//	.select("unix_timestamp_macro(col1) as col_ts, *")
		//	.groupBy("col_ts", "col_ts, sum(label) as c2")
		//	.select("to_timestamp_micro(col_ts),c2")
		//	.lazyPrint();

		//source
		//	.select("unix_timestamp_macro(col1) as col_ts, *")
		//	.groupBy("col_ts", "col_ts, sum(label) as c2, mtable_agg(col1, label)")
		//	.select("to_timestamp_micro(col_ts),c2")
		//	.lazyPrint();

		// fail
		//source
		//	.groupBy("col1", "sum(label) as c2")
		//	.lazyPrint();



		//Subject subject = new Subject()
		//	.addSubspace(new Subspace("label", 0))
		//	.setBreakdown(new Breakdown("col1"))
		//	.addMeasure(new Measure("col0", MeasureAggr.SUM));
		//
		//Subject subject = new Subject()
		//	.addSubspace(new Subspace("col1", new Timestamp(12)))
		//	.setBreakdown(new Breakdown("col1"))
		//	.addMeasure(new Measure("col0", MeasureAggr.SUM));
		//
		//Insight insight = Mining.calcInsight(source, subject, InsightType.Seasonality);
		//System.out.println(insight);

		LocalOperator.execute();
	}

	@Test
	@Ignore
	public void testChangePoint() {
		String filePath = "/Users/ning.cain/data/datav/changepoint.csv";
		String schema = "id int, data double";

		LocalOperator <?> data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true)
			.select("id, data, 0 as label");

		data.lazyPrint(2, "------ data -----");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("id"))
			.addMeasure(new Measure("data", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(data, subject, InsightType.ChangePoint);
		System.out.println(insight);
	}

	@Test
	@Ignore
	public void testCarSales() {
		String filePath = "/Users/ning.cain/data/datav/CarSales.csv";
		String schema = "year string, brand string, category string, model string, sales double";
		String[] brands = new String[] {"BMW", "Ford"};

		CsvSourceLocalOp data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);

		data.lazyPrint(2, "------ data -----");

		Subject subject = new Subject()
			.addSubspace(new Subspace("brand", "BMW"))
			.setBreakdown(new Breakdown("model"))
			.addMeasure(new Measure("sales", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(data, subject, InsightType.OutstandingNo1);
		System.out.println();
		System.out.println("------- BMW ------");
		System.out.println(insight);

		Subject subject2 = new Subject()
			.addSubspace(new Subspace("brand", "Ford"))
			.setBreakdown(new Breakdown("model"))
			.addMeasure(new Measure("sales", MeasureAggr.SUM));

		System.out.println();
		System.out.println("------- Ford ------");
		Insight insight2 = Mining.calcInsight(data, subject2, InsightType.OutstandingNo1);
		System.out.println(insight2);

		LocalOperator.execute();
	}

	@Test
	@Ignore
	public void testCarSales2() {
		String filePath = "/Users/ning.cain/data/datav/CarSales.csv";
		String schema = "year string, brand string, category string, model string, sales double";
		String[] brands = new String[] {"BMW", "Ford"};

		CsvSourceLocalOp data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);

		data.lazyPrint(2, "------ data -----");

		Subject subject = new Subject()
			.addSubspace(new Subspace("brand", "GMC"))
			.setBreakdown(new Breakdown("model"))
			.addMeasure(new Measure("sales", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(data, subject, InsightType.OutstandingNo1);
		System.out.println();
		System.out.println("------- BMW ------");
		System.out.println(insight);

		LocalOperator.execute();
	}

	@Test
	public void testTTest() {
		double df = 152.34;
		double t = 4.3251;
		System.out.println(CDF.studentT(t, df));
	}

	@Test
	public void testStdNormal() {
		double x = 3.1;
		System.out.println(2 * (1 - CDF.stdNormal(x)));
	}

}