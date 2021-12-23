package com.alibaba.alink.operator.common.timeseries.holtwinter;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType.SeasonalType;
import org.junit.Assert;
import org.junit.Test;

public class HoltWintersTest {
	@Test
	public void holtWintersTest() {

		double[] data = new double[] {11.0, 10.9, 10.7, 10.8, 10.7, 10.9, 10.9, 11.1, 11.0, 10.5};

		test(data, true, true,
			new double[] {10.884293837659598, 11.037355702715994, 11.014141618840004, 10.790999384185973});

		test(data, true, false,
			new double[] {10.156370713410478, 9.71153490461069, 9.266699095810903, 8.821863287011116});

		test(data, false, true,
			new double[] {10.884293837659598, 11.037355702715994, 11.014141618840004, 10.790999384185973});

		test(data, false, false,
			new double[] {10.839148711095714, 10.839148711095714, 10.839148711095714, 10.839148711095714});

	}

	@Test
	public void holtWintersTest2() {
		double[] data = new double[] {21.0, 20.9, 20.7, 20.8, 20.7, 20.9, 20.9, 21.1, 21.0, 20.5};

		test(data, true, true, null);

		test(data, true, false, null);

		test(data, false, true, null);

		test(data, false, false, null);
	}

	@Test
	public void holtWintersTest3() {

		double[] data1 = new double[] {495, 514, 542, 545, 563, 991, 1078, 1158, 970, 1058, 1028, 899, 4130, 5798,
			5459,
			5035, 8167, 7380, 6824, 5887, 5926, 5788, 5778, 4856, 6795, 5631, 4573, 4491, 4164, 4010, 4421, 6340, 6082,
			5998, 5214, 4624, 4415, 4348, 4699, 4688, 4085, 2869, 4476, 5020, 6774, 8386, 9358, 8596, 8542, 7688, 8057,
			17976, 13744, 14403, 14816, 17501, 15384, 15570, 24341, 21666, 33421, 21941, 20750, 18740, 17312, 18102,
			20557, 20964, 20089, 20168, 17794, 16338, 18172, 38693, 28597, 46593, 32874, 31723, 32689, 35030, 28246,
			24432, 16729, 16753, 15679, 18300, 18694, 17199, 19821, 21009, 21192, 19373, 17460, 20076, 19269, 17963,
			17922, 24011, 21512, 20982};
		int frequency = 4;
		double alpha = 0.3;
		double beta = 0.1;
		double gamma = 0.1;
		boolean doTrend = true;
		boolean doSeasonal = true;
		int predictNum = 28;

		double[] predictVals1 = holtWinters(
			data1,
			frequency,
			alpha,
			beta,
			gamma,
			doTrend,
			doSeasonal,
			SeasonalType.ADDITIVE,
			null, null, null,
			predictNum);

		System.out.println(new DenseVector(predictVals1));
	}

	private static double[] holtWinters(double[] historyVals,
										int frequency,
										double alpha,
										double beta,
										double gamma,
										boolean doTrend,
										boolean doSeasonal,
										SeasonalType seasonalType,
										Double levelStart,
										Double trendStart,
										double[] seasonalStart,
										int predictNum) {
		HoltWintersModel model = HoltWinters.fit(historyVals, frequency,
			alpha, beta, gamma, doTrend, doSeasonal,
			seasonalType, levelStart, trendStart, seasonalStart);

		return model.forecast(predictNum);
	}

	private double[] test(double[] data, boolean doTrend, boolean doSeasonal, double[] expectVals) {
		int frequency = 4;
		double alpha = 0.3;
		double beta = 0.1;
		double gamma = 0.1;
		int predictNum = 4;

		double[] predictVals = holtWinters(
			data,
			frequency,
			alpha,
			beta,
			gamma,
			doTrend,
			doSeasonal,
			SeasonalType.ADDITIVE,
			null, null, null,
			predictNum);

		System.out.println(new DenseVector(predictVals));

		if (expectVals != null) {
			Assert.assertArrayEquals(expectVals, predictVals, 10e-6);
		}

		return predictVals;

	}

}