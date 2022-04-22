package com.alibaba.alink.operator.common.outlier;

import junit.framework.TestCase;
import org.junit.Test;

public class DynamicTimeWarpingDetectorTest extends TestCase {

	@Test
	public void testDtw() {
		//double[] a = new double[] {1, 2, 3, 3, 5};
		//double[] b = new double[] {1, 2, 2, 2, 2, 2, 2, 4};
		double[] a = new double[] {5, 1, 2, 3, 4};
		double[] b = new double[] {5, 1, 2, 3, 10};

		System.out.println(DynamicTimeWarpingDetector.dtw(a, b, 3));
	}

}