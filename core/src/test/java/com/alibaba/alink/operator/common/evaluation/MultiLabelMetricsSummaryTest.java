package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.utils.JsonConverter;
import org.junit.Test;

import java.util.HashSet;

/**
 * Unit test for MultiLabelMetricsSummary.
 */
public class MultiLabelMetricsSummaryTest {
	@Test
	public void test() {
		String s = JsonConverter.toJson(new String[] {"a", "b", "c", "a"});
		//HashSet<EvaluationUtil.MultiLabelObject> set = JsonConverter.fromJson(JsonConverter.toJson(s),
		//    new TypeReference<HashSet<EvaluationUtil.MultiLabelObject>>() {}.getType());

		HashSet <Object> objects = JsonConverter.fromJson(s, HashSet.class);

		System.out.println(objects);
	}

}