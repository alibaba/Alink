package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.sink.CsvSinkLocalOp;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class AutoDiscoveryTest extends TestCase {

	@Ignore
	@Test
	public void test() throws Exception {
		//LocalOperator<?> data = Data.getCarSalesLocalSource();
		//LocalOperator<?> data = Data.getCensusLocalSource();
		LocalOperator<?> data = Data.getEmissionLocalSource();
		data.printStatistics();

		List <Insight> insights = AutoDiscovery.find(data, 60);

		for (Insight insight : insights) {
			System.out.println(insight);
		}
	}


	@Test
	public void testForTs() throws Exception {
		String filePath = getClass().getClassLoader().getResource("CarSales.csv").getPath();
		String schema = "year string, brand string, category string, model string, sales double";

		LocalOperator data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true)
			.select("to_timestamp_from_format(`year`, 'YYYY/MM/DD') as ts, brand, category, model, sales");

		List <Insight> insights = AutoDiscovery.find(data, 10);

		for (Insight insight : insights) {
			System.out.println(insight);
		}

		LocalOperator.execute();
	}

	@Ignore
	@Test
	public void test2() throws Exception {
		LocalOperator<?> data = Data.getCarSalesLocalSource();
		data.printStatistics("Data CarSales:");

		data.lazyPrint();

		Subject subject = new Subject()
			//.addSubspace(new Subspace("category", "Compact"))
			.setBreakdown(new Breakdown("category"))
			.addMeasure(new Measure("sales", MeasureAggr.MAX));

		Insight insight = Mining.calcInsight(data, subject, InsightType.Evenness);
		System.out.println(insight);

		LocalOperator <?> sinkOp = new CsvSinkLocalOp()
			.setFilePath("/Users/ning.cain/data/datav/1")
			.setOverwriteSink(true);

		LayoutData layout = (LayoutData) insight.layout;
		new MemSourceLocalOp(layout.data).link(sinkOp);

		LocalOperator.execute();
	}
}