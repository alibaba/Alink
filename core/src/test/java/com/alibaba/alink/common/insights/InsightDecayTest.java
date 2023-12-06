package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

public class InsightDecayTest {

	@Test
	@Ignore
	public void testDecay() throws Exception {
		//LocalOperator <?> data = Data.getEmissionLocalSource();
		LocalOperator.setParallelism(4);
		//data: https://www.kaggle.com/c/criteo-display-ad-challenge/data
		String filePath = "https://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_10w_test_data";
		//String filePath = "http://alink-example-data.oss-cn-hangzhou-zmf.aliyuncs.com/criteo_random_90w_train_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_10w_test_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_90w_train_data";
		//String filePath = "/Users/ning.cain/data/datav/criteo_random_100w_data";

		String schemaStr = "label int,nf01 int,nf02 int,nf03 int,nf04 int,nf05 int,nf06 int,nf07 int,nf08 int,nf09 "
			+ "int,nf10 int,nf11 int,nf12 int,nf13 int,cf01 string,cf02 string,cf03 string,cf04 string,cf05 string,"
			+ "cf06 string,cf07 string,cf08 string,cf09 string,cf10 string,cf11 string,cf12 string,cf13 string,cf14 "
			+ "string,cf15 string,cf16 string,cf17 string,cf18 string,cf19 string,cf20 string,cf21 string,cf22 string,"
			+ "cf23 string,cf24 string,cf25 string,cf26 string";

		LocalOperator <?> data = new CsvSourceLocalOp()
			.setFilePath(filePath)
			.setSchemaStr(schemaStr);

		List <Insight> insights = AutoDiscovery.find(data, 60);

		InsightDecay insightDecay = new InsightDecay();

		for (Insight insight : insights) {
			insight.score *= insightDecay.getInsightDecay(insight);
		}

		insights.sort(new Comparator <Insight>() {
			@Override
			public int compare(Insight o1, Insight o2) {
				return -Double.compare(o1.score, o2.score);
			}
		});

		for (int i = 0; i < insights.size() && i <= 100; i++) {
			System.out.println(insights.get(i));
		}

		LocalOperator.execute();
	}

	@Test
	@Ignore
	public void testWithoutDecay() throws Exception {
		LocalOperator <?> data = Data.getEmissionLocalSource();
		List <Insight> insights = AutoDiscovery.find(data, 20);

		for (int i = 0; i < insights.size() && i <= 100; i++) {
			System.out.println(insights.get(i));
		}

		LocalOperator.execute();
	}
}