package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class SarimaTest extends AlinkTestBase {

	@Test
	public void test() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.1, 5.0, 5.0, 4.9, 4.9};
		int p = 2;
		int d = 2;
		int q = 0;
		int sP = 0;
		int sD = 0;
		int sQ = 0;
		int seasonalPeriod = 1;
		int ifIntercept = 0;
		EstMethod estMethod = EstMethod.CssMle;
		SarimaModel model = Sarima.fit(data, p, d, q, sP, sD, sQ, estMethod, ifIntercept, seasonalPeriod);

		double[] predValues = model.forecast(4).get(0);
		System.out.println(new DenseVector(predValues));
	}

	@Test
	public void test2() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};
		int p = 2;
		int d = 2;
		int q = 0;
		int sP = 1;
		int sD = 1;
		int sQ = 1;
		int seasonalPeriod = 3;
		int ifIntercept = 0;
		EstMethod estMethod = EstMethod.CssMle;
		SarimaModel model = Sarima.fit(data, p, d, q, sP, sD, sQ, estMethod, ifIntercept, seasonalPeriod);

		Assert.assertArrayEquals(
			new double[] {5.1700737414409, 5.287740736359297, 5.016494269506571, 4.962504932599818},
			model.forecast(4).get(0),
			10e-6);
	}

	@Test
	public void test3() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};
		int maxOrder = 2;
		int maxSeasonalOrder = 2;
		IcType ic = IcType.AIC;
		EstMethod estMethod = EstMethod.CssMle;
		int seasonalPeriod = 7;

		SarimaModel arima = Sarima.autoFit(
			data,
			maxOrder,
			maxSeasonalOrder,
			estMethod,
			ic,
			seasonalPeriod);

		double[] predictVals = arima.forecast(4).get(0);
		System.out.println(new DenseVector(predictVals));
	}

	@Test
	public void test4() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};
		int p = 0;
		int d = 1;
		int q = 1;
		int sP = 0;
		int sD = 0;
		int sQ = 0;
		int seasonalPeriod = 7;
		int ifIntercept = 0;
		EstMethod estMethod = EstMethod.CssMle;
		SarimaModel model = Sarima.fit(data, p, d, q, sP, sD, sQ, estMethod, ifIntercept, seasonalPeriod);

		double[] predValues = model.forecast(4).get(0);
		System.out.println(new DenseVector(predValues));
	}

}