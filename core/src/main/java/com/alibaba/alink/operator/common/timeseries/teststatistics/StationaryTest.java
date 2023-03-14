package com.alibaba.alink.operator.common.timeseries.teststatistics;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.probabilistic.IDF;
import com.alibaba.alink.operator.common.statistics.DistributionFuncName;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

public class StationaryTest {

	public StationaryTest() {
	}

	public double[] difference(double[] data) {
		double[] diffData = new double[data.length];
		for (int i = 1; i < data.length; i++) {
			diffData[i] = data[i] - data[i - 1];
		}
		return diffData;
	}

	public double[] differenceFromZero(double[] data) {
		double[] diffData = new double[data.length - 1];
		for (int i = 0; i < diffData.length; i++) {
			diffData[i] = data[i + 1] - data[i];
		}
		return diffData;
	}

	/**
	 * data is the sample data for model.
	 * alpha is the significant level, for example, 0.95 for the level of 95%
	 * h is the lag to be tested that if there is no correlation between sample.
	 * Suggestion on h: Ln(data.length), 20, or lags of order
	 * Ljung-Box test might not perform an asymptotic chi-square distribution under the null
	 */
	public double[][] ljungBox(double[] data, double alpha, int h) {
		if (h >= data.length) {
			throw new AkIllegalDataException("Lag h to be tested must be smaller than the number of sample.");
		}

		//1st row is Q value, 2rd row is value of Chi2 at alpha with h degree of freedom
		double[][] result = new double[2][1];

		int n = data.length;
		double[] acf = TsMethod.acf(data, h).get(0);
		double q = 0;
		for (int j = 1; j <= h; j++) {
			q = q + acf[j] / (n - j);
		}
		q = q * (n + 2) * n;
		result[0][0] = q;

		IDF idf = new IDF(DistributionFuncName.Chi2, new double[] {h});
		double chi2Value = idf.calculate(alpha);
		result[1][0] = chi2Value;

		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			if (q > chi2Value) {
				System.out.println("Reject the null hypothesis that the series is stationary");
			} else {
				System.out.println("Do not reject the null hypothesis that the series is stationary");
			}
		}

		return result;
	}

	public static KPSS kpss(double[] data) {
		KPSS kpss = new KPSS();
		kpss.kpssTest(data.clone(), 1, 1);
		return kpss;
	}
}




