package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * UT for SimilarityUtil
 */

public class SimilarityUtilTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void test() {
		PriorityQueue <Tuple2 <Double, Object>> map = new PriorityQueue <>(Comparator.comparingDouble(v -> v.f0));
		Tuple2 <Double, Object> t = SimilarityUtil.updateQueue(map, 5, null, Tuple2.of(null, null));
		Assert.assertNull(t.f0);
		Assert.assertNull(t.f1);

		thrown.expect(RuntimeException.class);
		SimilarityUtil.toInteger(5L);
	}

}