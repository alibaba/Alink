package com.alibaba.alink.common.insights;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.local.LocalOperator;
import org.junit.Test;

public class StatInsightTest {
	@Test
	public void testBasicStat() throws Exception {
		LocalOperator <?> data = Data.getCarSalesLocalSource();
		Insight insight = StatInsight.basicStat(data, "sales");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(insight.layout.data));
	}

	@Test
	public void testBasicStat2() throws Exception {
		LocalOperator <?> data = Data.getCarSalesLocalSource();
		Insight insight = StatInsight.basicStat(data, "brand");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(insight.layout.data));
	}

	@Test
	public void testDistribution() throws Exception {
		LocalOperator <?> data = Data.getCarSalesLocalSource();
		Insight insight = StatInsight.distribution(data, new Breakdown("category"), "sales");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(insight.layout.data));
	}

	@Test
	public void testDistribution2() throws Exception {
		LocalOperator <?> data = Data.getCarSalesLocalSource();
		Insight insight = StatInsight.distribution(data, new Breakdown("brand"), "category");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(insight.layout.data));
	}
}
