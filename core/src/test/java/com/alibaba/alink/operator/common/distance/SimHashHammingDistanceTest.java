package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.operator.common.similarity.Sample;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigInteger;

public class SimHashHammingDistanceTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private SimHashHammingDistance distance = new SimHashHammingDistance(64);

	@Test
	public void hammingDistance() {
		BigInteger b1 = new BigInteger("16");
		BigInteger b2 = new BigInteger("44");

		Assert.assertEquals(distance.hammingDistance(b1, b2), 4);
		Assert.assertEquals(distance.simHash(null), BigInteger.ZERO);

		thrown.expect(RuntimeException.class);
		SimHashHammingDistance distance = new SimHashHammingDistance(16);
	}

	@Test
	public void testString() {
		Assert.assertEquals(distance.calc("abc", "abe"), 6.0, 0.1);
		Assert.assertEquals(distance.calc(new String[] {"a", "b", "c"}, new String[] {"a", "b", "e"}), 6.0, 0.1);

	}

	@Test
	public void testSample() {
		Sample sample = new Sample("abc", null);
		distance.updateLabel(sample, "abc");
		Assert.assertEquals(sample.getLabel(), new BigInteger("12399407586"));
		Assert.assertEquals(distance.calc(sample, sample, false), 0.0, 0.01);
	}
}