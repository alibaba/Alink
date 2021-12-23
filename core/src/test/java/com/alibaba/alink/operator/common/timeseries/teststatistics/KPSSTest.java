package com.alibaba.alink.operator.common.timeseries.teststatistics;

import org.junit.Assert;
import org.junit.Test;

public class KPSSTest {
	@Test
	public void test() {
		double[] vals = new double[] {12.4, 12.3, 12.2, 12.1, 11.9, 11.8, 11.7, 11.7, 11.6, 11.4};
		KPSS kpss = new KPSS();
		kpss.kpssTest(vals, 1, 1);

		Assert.assertEquals(0.41070625281151535, kpss.cnValue, 10e-10);
		Assert.assertEquals(0.4037197768133954, kpss.ctValue, 10e-10);
	}

	@Test
	public void test2() {
		double[] vals = new double[] {18.6,22.2,19.4,19.7,18.5,18.0,22.3,19.5,18.6,21.3};
		KPSS kpss = new KPSS();
		kpss.kpssTest(vals, 1, 1);

		System.out.println(kpss.cnValue);
		System.out.println(kpss.ctValue);
	}
}