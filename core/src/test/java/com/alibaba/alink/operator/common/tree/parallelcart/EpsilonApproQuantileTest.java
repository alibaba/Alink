package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.utils.JsonConverter;
import org.junit.Test;

public class EpsilonApproQuantileTest {

	@Test
	public void testLimitSize() {
		EpsilonApproQuantile.QuantileSketch quantileSketch = new EpsilonApproQuantile.QuantileSketch();

		for (int i = 1; i < 10000000; i += 10000) {
			for (double j = 0.1; j < 1; j += 0.1) {
				quantileSketch.limitSizeLevel(i, j);

				System.out.println(String.format("i: %d, j: %f, %s", i, j, JsonConverter.toJson(quantileSketch)));
			}
		}
	}

}