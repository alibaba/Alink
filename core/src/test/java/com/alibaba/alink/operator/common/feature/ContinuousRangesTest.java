package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.Types;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * UT for ContinuousRanges
 */
public class ContinuousRangesTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void test() {
		ContinuousRanges ranges = new ContinuousRanges("f0", Types.LONG, new Number[] {0.0, 0.1}, true);
		Assert.assertTrue(ranges.isFloat());

		ranges = new ContinuousRanges("f0", Types.LONG, new Number[] {0.0, 1L}, true);
		Assert.assertTrue(ranges.isFloat());

		ranges = new ContinuousRanges("f0", Types.DOUBLE, new Number[] {0L, 1L}, true);
		Assert.assertTrue(ranges.isFloat());

		ranges = new ContinuousRanges("f0", Types.LONG, new Number[] {0L, 1L}, true);
		Assert.assertFalse(ranges.isFloat());
		Assert.assertEquals(ranges.getIntervalNum(), 3);

		ranges = ContinuousRanges.deSerialize(null);
		Assert.assertNull(ranges);

		thrown.expect(IllegalArgumentException.class);
		ranges = ContinuousRanges.deSerialize("a");
	}

}