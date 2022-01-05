package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class DLLauncherUtilsTest extends AlinkTestBase {

	@Test(expected = RuntimeException.class)
	public void testAdjustNumWorkersPSsNull_Null_Minus1() {
		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(null, null, -1);
	}

	@Test()
	public void testAdjustNumWorkersPSsNull_Null_3() {
		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(null, null, 3);
		Assert.assertEquals(Tuple2.of(2, 1), numWorkersPSsTuple);
	}

	@Test()
	public void testAdjustNumWorkersPSsNull_Null_103() {
		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(null, null, 103);
		Assert.assertEquals(Tuple2.of(78, 25), numWorkersPSsTuple);
	}

	@Test(expected = RuntimeException.class)
	public void testAdjustNumWorkersPSs3_Null_Minus1() {
		DLLauncherUtils.adjustNumWorkersPSs(3, null, -1);
	}

	@Test
	public void testAdjustNumWorkersPSs3_Null_5() {
		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(3, null, 5);
		Assert.assertEquals(Tuple2.of(3, 2), numWorkersPSsTuple);
	}

	@Test
	public void testAdjustNumWorkersPSsNull_3_5() {
		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(null, 3, 5);
		Assert.assertEquals(Tuple2.of(2, 3), numWorkersPSsTuple);
	}
}