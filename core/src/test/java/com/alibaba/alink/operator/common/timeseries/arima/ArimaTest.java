package com.alibaba.alink.operator.common.timeseries.arima;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;
import org.junit.Assert;
import org.junit.Test;

public class ArimaTest {
	@Test
	public void test() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};
		int p = 2;
		int d = 2;
		int q = 0;
		EstMethod estMethod = EstMethod.CssMle;
		int ifIntercept = 0;
		ArimaModel arima = Arima.fit(data, p, d, q, estMethod, ifIntercept);

		double[] predictVals = arima.forecast(4).get(0);

		Assert.assertArrayEquals(
			new double[] {4.020751429242694, 5.273582695643859, 5.1151057827452275, 6.420365254983571},
			predictVals, 10e-6);
	}

	@Test
	public void testAll() {
		testArima(EstMethod.CssMle);
		testArima(EstMethod.Css);
		testArima(EstMethod.Mom);
		testArima(EstMethod.Hr);
	}

	private static void testArima(EstMethod estMethod) {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};
		int p = 2;
		int d = 2;
		int q = 0;
		int ifIntercept = 0;
		ArimaModel arima = Arima.fit(data, p, d, q, estMethod, ifIntercept);

		double[] predictVals = arima.forecast(4).get(0);

		System.out.println(new DenseVector(predictVals));
	}

	@Test
	public void test2() {
		double[] data = new double[] {0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.6,
			1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6, 1.6};

		int p = 1;
		int d = 1;
		int q = 1;

		EstMethod estMethod = EstMethod.CssMle;
		int ifIntercept = 0;
		ArimaModel arima = Arima.fit(data, p, d, q, estMethod, ifIntercept);

		double[] predictVals = arima.forecast(4).get(0);

		System.out.println(new DenseVector(predictVals));
	}

	@Test
	public void test3() {
		double[] data = new double[] {5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9,
			5.7, 5.5, 5.4, 5.3, 5.2, 5.2, 5.0, 5.0, 4.9, 4.9};

		ArimaModel arima = Arima.autoFit(data);

		double[] predictVals = arima.forecast(4).get(0);

		System.out.println(new DenseVector(predictVals));

	}
}