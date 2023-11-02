package com.alibaba.alink.common.insights;

import junit.framework.TestCase;
import org.junit.Test;

public class MiningTest extends TestCase {

	@Test
	public void testOutstandingTop1() {
		double[] values = new double[] {110774, 1529904, 153476, 3230050, 858885, 280401, 557569, 1132070};
		double sum = 0;
		double max = 0;
		for (int i = 0; i < values.length; i++) {
			sum += values[i];
			if (values[i] > max) {
				max = values[i];
			}
		}
		double mean = sum / values.length;
		System.out.println("mean: " + mean);

		double step = 1000000;
		Double[] valuesForTest = new Double[values.length];
		for (int i = 0; i < values.length; i++) {
			valuesForTest[i] = (values[i] - mean)/step;
			System.out.println(valuesForTest[i]);
		}

		double maxForTest = (max - mean)/step;
		double pvalue = Mining.outstandingNo1PValue(valuesForTest, 0.7, maxForTest);
		System.out.println("pvalue: " + pvalue);

	}

}