package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * UT for UvCounter.
 */
public class UvCounterTest extends AlinkTestBase {

	@Test
	public void test() {
		UvCounter counter1 = new UvCounter();
		UvCounter counter2 = new UvCounter();

		for (int i = 0; i < 1000; i++) {
			counter1.visit(i);
			counter2.visit(i * 2);
		}

		AbstractCounter counter = counter1.merge(counter2);
		Assert.assertEquals(counter.count(), 1500);
	}

}