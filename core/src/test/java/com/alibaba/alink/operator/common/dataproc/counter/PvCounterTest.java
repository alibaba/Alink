package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * UT for PvCounter
 */
public class PvCounterTest extends AlinkTestBase {
	@Test
	public void test() {
		PvCounter counter1 = new PvCounter(0);
		PvCounter counter2 = new PvCounter();

		for (int i = 0; i < 1000; i++) {
			counter1.visit(i);
			counter2.visit(i * 2);
		}

		AbstractCounter counter = counter1.merge(counter2);
		Assert.assertEquals(counter.count(), 2000);
	}

}