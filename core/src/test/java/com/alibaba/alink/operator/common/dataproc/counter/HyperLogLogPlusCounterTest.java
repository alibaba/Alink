package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * UT for HyperLogLogPlusCounter
 */
public class HyperLogLogPlusCounterTest extends AlinkTestBase {
	@Test
	public void test() {
		HyperLogLogPlusCounter counter1 = new HyperLogLogPlusCounter(7);
		HyperLogLogPlusCounter counter2 = new HyperLogLogPlusCounter(counter1);

		for (int i = 0; i < 1000; i++) {
			counter1.visit(i);
			counter2.visit(i * 2);
		}

		AbstractCounter counter = counter1.merge(counter2);
		Assert.assertTrue(counter.count() > 1000 && counter.count() < 2000);
	}

	@Test
	public void testSparse() {
		HyperLogLogPlusCounter counter1 = new HyperLogLogPlusCounter("SPARSE", 11, 30);
		HyperLogLogPlusCounter counter2 = new HyperLogLogPlusCounter(counter1);

		for (int i = 0; i < 1000; i++) {
			counter1.visit(String.valueOf(i));
			counter2.visit(String.valueOf(i * 2));
		}

		AbstractCounter counter = counter1.merge(counter2);
		System.out.println(counter.count());
		Assert.assertTrue(counter.count() >= 1000 && counter.count() < 2000);
	}

	@Test
	public void testNormalSparse() {
		HyperLogLogPlusCounter counter1 = new HyperLogLogPlusCounter(11);
		HyperLogLogPlusCounter counter2 = new HyperLogLogPlusCounter("SPARSE", 11, 20);

		for (int i = 0; i < 1000; i++) {
			counter1.visit(String.valueOf(i));
			counter2.visit(String.valueOf(i * 2));
		}

		AbstractCounter counter = counter1.merge(counter2);
		Assert.assertTrue(counter.count() > 1000 && counter.count() < 2000);

		counter = counter2.merge(counter1);
		Assert.assertTrue(counter.count() > 1000 && counter.count() < 2000);
	}

}