package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.sink.CsvSinkLocalOp;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.sql.GroupByLocalOp2;
import org.junit.Ignore;
import org.junit.Test;
import scala.concurrent.java8.FuturesConvertersImpl.P;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

public class AutoDiscoveryTest {
	@Test
	@Ignore
	public void testCriteo() throws Exception {
		//String outputFileName = "/Users/ning.cain/data/datav/tmp/1.log";
		//OutputStream outputstream = new FileOutputStream(outputFileName);
		//PrintStream printstream = new PrintStream(outputstream);
		//System.setErr(printstream);
		//System.setOut(printstream);

		//LocalOperator.setParallelism(1);

		//data: https://www.kaggle.com/c/criteo-display-ad-challenge/data
		String filePath = "https://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_10w_test_data";
		//String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_90w_train_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_10w_test_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_90w_train_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_100w_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_200w_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		LocalOperator <?> data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);

		List <Insight> insights = AutoDiscovery.find(data, 120);

		for (Insight insight : insights) {
			System.out.println(insight);
		}

		LocalOperator.execute();

		System.out.println("insight.size(): " + insights.size());
	}

	@Test
	@Ignore
	public void testForGroupby() throws Exception {
		//LocalOperator.setParallelism(4);
		//type 1: 31s typ2:33s type 3: 18s
		// data: https://www.kaggle.com/c/criteo-display-ad-challenge/data
		//String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_10w_test_data";
		//String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_90w_train_data";

		String filePath = "/Users/ning.cain/data/datav/criteo_random_90w_train_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		LocalOperator <?> data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);

		String selectClause = "`cf01`, "
			+ "COUNT(`cf01`) AS measure_0, "
			+ "SUM(`label`) AS measure_1, "
			+ "AVG(`label`) AS measure_2, "
			+ "MIN(`label`) AS measure_3, "
			+ "MAX(`label`) AS measure_4, "
			+ "SUM(`nf01`) AS measure_5, "
			+ "AVG(`nf01`) AS measure_6, "
			+ "MIN(`nf01`) AS measure_7, "
			+ "MAX(`nf01`) AS measure_8, "
			+ "SUM(`nf02`) AS measure_9, "
			+ "AVG(`nf02`) AS measure_10, "
			+ "MIN(`nf02`) AS measure_11, "
			+ "MAX(`nf02`) AS measure_12, "
			+ "SUM(`nf03`) AS measure_13, "
			+ "AVG(`nf03`) AS measure_14, "
			+ "MIN(`nf03`) AS measure_15, "
			+ "MAX(`nf03`) AS measure_16, "
			+ "SUM(`nf04`) AS measure_17, "
			+ "AVG(`nf04`) AS measure_18, "
			+ "MIN(`nf04`) AS measure_19, "
			+ "MAX(`nf04`) AS measure_20, "
			+ "SUM(`nf05`) AS measure_21, "
			+ "AVG(`nf05`) AS measure_22, "
			+ "MIN(`nf05`) AS measure_23, "
			+ "MAX(`nf05`) AS measure_24, "
			+ "SUM(`nf06`) AS measure_25, "
			+ "AVG(`nf06`) AS measure_26, "
			+ "MIN(`nf06`) AS measure_27, "
			+ "MAX(`nf06`) AS measure_28, "
			+ "SUM(`nf07`) AS measure_29, "
			+ "AVG(`nf07`) AS measure_30, "
			+ "MIN(`nf07`) AS measure_31, "
			+ "MAX(`nf07`) AS measure_32, "
			+ "SUM(`nf08`) AS measure_33, "
			+ "AVG(`nf08`) AS measure_34, "
			+ "MIN(`nf08`) AS measure_35, "
			+ "MAX(`nf08`) AS measure_36, "
			+ "SUM(`nf09`) AS measure_37, "
			+ "AVG(`nf09`) AS measure_38, "
			+ "MIN(`nf09`) AS measure_39, "
			+ "MAX(`nf09`) AS measure_40, "
			+ "SUM(`nf10`) AS measure_41, "
			+ "AVG(`nf10`) AS measure_42, "
			+ "MIN(`nf10`) AS measure_43, "
			+ "MAX(`nf10`) AS measure_44, "
			+ "SUM(`nf11`) AS measure_45, "
			+ "AVG(`nf11`) AS measure_46, "
			+ "MIN(`nf11`) AS measure_47, "
			+ "MAX(`nf11`) AS measure_48, "
			+ "SUM(`nf12`) AS measure_49, "
			+ "AVG(`nf12`) AS measure_50, "
			+ "MIN(`nf12`) AS measure_51, "
			+ "MAX(`nf12`) AS measure_52, "
			+ "SUM(`nf13`) AS measure_53, "
			+ "AVG(`nf13`) AS measure_54, "
			+ "MIN(`nf13`) AS measure_55, "
			+ "MAX(`nf13`) AS measure_56";

		String groupClause = "cf01";

		LocalOperator <?> groupOp = new GroupByLocalOp2()
			.setGroupByPredicate(groupClause)
			.setSelectClause(selectClause);

		data.link(groupOp);

		groupOp.printStatistics();

		LocalOperator.execute();

		///////////////for batch compare////////////////
		//BatchOperator <?> dataBatch = new CsvSourceBatchOp()
		//	.setFilePath(filePath)
		//	.setSchemaStr(schemaStr);
		//
		//BatchOperator <?> groupOpBatch = new GroupByBatchOp()
		//	.setGroupByPredicate(groupClause)
		//	.setSelectClause(selectClause);
		//
		//dataBatch.link(groupOpBatch);
		//
		//groupOpBatch.lazyPrintStatistics();
		//
		//BatchOperator.execute();

	}

	@Test
	@Ignore
	public void test() throws Exception {
		//LocalOperator<?> data = Data.getCarSalesLocalSource();
		//LocalOperator<?> data = Data.getCensusLocalSource();
		LocalOperator <?> data = Data.getEmissionLocalSource();
		data.printStatistics();

		List <Insight> insights = AutoDiscovery.find(data, 1);

		for (Insight insight : insights) {
			System.out.println(insight);
		}
	}

	@Test
	@Ignore
	public void testForTs() throws Exception {
		LocalOperator data = Data.getCarSalesLocalSource()
			.select("to_timestamp_from_format(`year`, 'YYYY/MM/DD') as ts, brand, category, model, sales")
			.printStatistics();

		List <Insight> insights = AutoDiscovery.find(data, 10);

		for (Insight insight : insights) {
			System.out.println(insight);
		}

		LocalOperator.execute();
	}

	@Test
	@Ignore
	public void test2() throws Exception {
		//LocalOperator <?> data = Data.getCarSalesLocalSource();
		LocalOperator <?> data = get10wData();
		//data.printStatistics("Data :");

		//data.lazyPrint();

		Subject subject = new Subject()
			//.addSubspace(new Subspace("cf17", "776ce399"))
			.setBreakdown(new Breakdown("cf06"))
			.addMeasure(new Measure("nf02", MeasureAggr.COUNT));

		//data = data.select("brand, category, -sales as sales");
		//Subject subject = new Subject()
		//	.addSubspace(new Subspace("brand", "BMW"))
		//	.setBreakdown(new Breakdown("category"))
		//	.addMeasure(new Measure("sales", MeasureAggr.AVG));

		Insight insight = Mining.calcInsight(data, subject, InsightType.OutstandingTop2);
		System.out.println(insight);

		//LocalOperator <?> sinkOp = new CsvSinkLocalOp()
		//	.setFilePath("/Users/ning.cain/data/datav/1")
		//	.setOverwriteSink(true);

		//LayoutData layout = (LayoutData) insight.layout;
		//new MemSourceLocalOp(layout.data).link(sinkOp);

		LocalOperator.execute();
	}

	private static LocalOperator <?> get10wData() {
		String filePath = "/Users/ning.cain/data/datav/criteo_random_10w_test_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		LocalOperator <?> data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);

		return data;
	}
}