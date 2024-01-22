package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.common.utils.JsonConverter;
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
	public void testOutstandingTop1() {
		double[] values = new double[] {110774, 1529904, 153476, 3230050, 858885, 280401, 557569, 1132070};
		double sum = 0;
		double max = 0;
		for (int i = 0; i < values.length; i++) {
			sum += values[i];
			if (values[i] > max) {
				max = values[i];
			}
		}
		double mean = sum / values.length;
		System.out.println("mean: " + mean);

		double step = 1000000;
		Double[] valuesForTest = new Double[values.length];
		for (int i = 0; i < values.length; i++) {
			valuesForTest[i] = (values[i] - mean) / step;
			System.out.println(valuesForTest[i]);
		}

		double maxForTest = (max - mean) / step;
		double pvalue = Mining.outstandingNo1PValue(valuesForTest, 0.7, maxForTest);
		System.out.println("pvalue: " + pvalue);

	}

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

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingNo1);
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	@Ignore
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
		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	public void testEveness() {
		List <Row> rows = new ArrayList <>();
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

		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	public void testOutstandingTop2() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "1", 0));
		rows.add(Row.of(14, "7", 0));
		rows.add(Row.of(1, "2", 0));
		rows.add(Row.of(3, "3", 0));
		rows.add(Row.of(4, "4", 0));
		rows.add(Row.of(5, "5", 0));
		rows.add(Row.of(6, "6", 0));
		rows.add(Row.of(6, "8", 0));
		rows.add(Row.of(6, "9", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingTop2);
		System.out.println(insight);

		System.out.println(JsonConverter.toJson(insight));
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

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingNo1);
		System.out.println(insight);
		Insight insight2 = Mining.calcInsight(source, subject, InsightType.Attribution);
		System.out.println(insight2);

		System.out.println(JsonConverter.toJson(insight2));
	}

	@Test
	public void testOutlier() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(-10000, new Timestamp(1000), 0));
		rows.add(Row.of(-1, new Timestamp(2000), 0));
		rows.add(Row.of(-3, new Timestamp(3000), 0));
		rows.add(Row.of(-4, new Timestamp(4000), 0));
		rows.add(Row.of(-5, new Timestamp(5000), 0));
		rows.add(Row.of(-6, new Timestamp(6000), 0));
		rows.add(Row.of(-1, new Timestamp(7000), 0));
		rows.add(Row.of(-3, new Timestamp(8000), 0));
		rows.add(Row.of(-4, new Timestamp(9000), 0));
		rows.add(Row.of(-5, new Timestamp(10000), 0));
		rows.add(Row.of(-6, new Timestamp(11000), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			//.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Outlier);
		System.out.println(insight);

		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	public void testOTrend() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, new Timestamp(1000), 0));
		rows.add(Row.of(2, new Timestamp(2000), 0));
		rows.add(Row.of(3, new Timestamp(3000), 0));
		rows.add(Row.of(4, new Timestamp(4000), 0));
		rows.add(Row.of(5, new Timestamp(5000), 0));
		rows.add(Row.of(6, new Timestamp(6000), 0));
		rows.add(Row.of(1, new Timestamp(7000), 0));
		rows.add(Row.of(7, new Timestamp(8000), 0));
		rows.add(Row.of(8, new Timestamp(9000), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Trend);
		System.out.println(insight);

		//System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	public void testSeasonality() {
		List <Row> rows = new ArrayList <>();

		//long step = 86400;
		long step = 60 * 60 * 24;
		rows.add(Row.of(1, new Timestamp(1000 * step), 0));
		rows.add(Row.of(2, new Timestamp(2000 * step), 0));
		rows.add(Row.of(3, new Timestamp(3000 * step), 0));
		rows.add(Row.of(4, new Timestamp(4000 * step), 0));
		rows.add(Row.of(1, new Timestamp(5000 * step), 0));
		rows.add(Row.of(2, new Timestamp(6000 * step), 0));
		rows.add(Row.of(3, new Timestamp(7000 * step), 0));
		rows.add(Row.of(4, new Timestamp(8000 * step), 0));
		rows.add(Row.of(1, new Timestamp(9000 * step), 0));
		rows.add(Row.of(2, new Timestamp(10000 * step), 0));
		rows.add(Row.of(3, new Timestamp(11000 * step), 0));
		rows.add(Row.of(4, new Timestamp(12000 * step), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Seasonality);
		System.out.println(insight);

		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	public void testTs() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, new Timestamp(2000), 1));
		rows.add(Row.of(2, new Timestamp(2000), 2));
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
		//source.lazyPrint();
		//source.select("*, label + 1 as f0").lazyPrint();
		//source.select("date_format_ltz(col1) as c1").lazyPrint();
		//source
		//	.select("col1,TIMESTAMP'1970-01-01 00:00:00.012' as col2, CAST('1970-01-01 00:00:00.012' AS TIMESTAMP) as
		//	col3,label")
		//	//.lazyPrint()
		//	////.select(new String[]{"col1"})
		//	//.select("col1,col2")
		//	//.select("col1")
		//	.filter("unix_timestamp_macro(col1)=1000")
		//	//.filter("col1=CAST('1970-01-01 00:00:01' AS TIMESTAMP)")
		//	//.filter("col1=TIMESTAMP'1970-01-01 00:00:01'")
		//	.filter("col1=to_timestamp_from_format('1970-01-01 00:00:01', 'yyyy-MM-dd hh:mm:ss')")
		//	.lazyPrint();

		LocalOperator <?> t1 = source
			.select("*, col1 as col1_bak") //ok
			//.filter("col1 > to_timestamp_from_format('1970-01-01 00:00:01', 'yyyy-MM-dd hh:mm:ss')") // fail
			//.filter("unix_timestamp_macro(col1) > "
			//	+ "unix_timestamp_macro(to_timestamp_from_format('1970-01-01 00:00:01', 'yyyy-MM-dd hh:mm:ss'))") // ok
			//.filter("col1 > TIMESTAMP '1970-01-01 00:00:01'") // ok
			//.filter("col1 > cast('1970-01-01 00:00:01' as TIMESTAMP)") // ok
			;

		t1.lazyPrint();

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
			//.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("id"))
			.addMeasure(new Measure("data", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(data, subject, InsightType.ChangePoint);
		System.out.println(insight);

		System.out.println(JsonConverter.toJson(insight));
	}

	@Test
	@Ignore
	public void testChangePointSmall() {
		List <Row> rows = new ArrayList <>();

		long step = 1;
		rows.add(Row.of(1, new Timestamp(1000 * step), 0));
		rows.add(Row.of(3, new Timestamp(2000 * step), 0));
		rows.add(Row.of(5, new Timestamp(3000 * step), 0));
		rows.add(Row.of(3, new Timestamp(4000 * step), 0));
		rows.add(Row.of(1, new Timestamp(5000 * step), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.ChangePoint);
		System.out.println(insight);
	}

	@Test
	public void testCarSales() {
		LocalOperator <?> data = Data.getCarSalesLocalSource();

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
	public void testCarSales2() {
		LocalOperator <?> data = Data.getCarSalesLocalSource();

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