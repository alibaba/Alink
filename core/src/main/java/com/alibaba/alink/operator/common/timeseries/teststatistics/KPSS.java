package com.alibaba.alink.operator.common.timeseries.teststatistics;

import com.alibaba.alink.operator.common.timeseries.TsMethod;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class KPSS {
	private int cnBandwidth;
	private int ctBandwidth;
	public double cnValue;
	protected double ctValue;
	public double cnCritic;
	private double ctCritic;
	private String cnResult;
	private String ctResult;

	private double gammaFunc(int j, int t, double[] e) {
		double gamma = 0;
		for (int i = j; i < t; i++) {
			gamma = gamma + e[i] * e[i - j];
		}
		return gamma / t;
	}

	/**
	 * kernel=1: Bartllet. kernel=2: Quadratic Spectral
	 */
	int autoBandwidth(double[] e, int t, int kernel) {
		//step 1
		double n = 0;
		if (kernel == 1) {
			n = Math.floor(Math.pow(t, 0.22222222));
		}
		if (kernel == 2) {
			n = Math.floor(Math.pow(t, 0.08));
		}

		double s0 = this.gammaFunc(0, t, e);
		for (int i = 1; i <= n; i++) {
			s0 = s0 + 2 * this.gammaFunc(i, t, e);
		}

		double gamma = 0;
		int m = 0;
		if (kernel == 1) {
			double s1 = 0;
			for (int i = 1; i <= n; i++) {
				s1 = s1 + 2 * this.gammaFunc(i, t, e) * i;
			}

			gamma = 1.1447 * Math.pow(Math.pow((s1 / s0), 2), 0.33333333);

			m = (int) Math.round(Math.min(t, gamma * Math.pow(t, 0.3333333)));
		}

		if (kernel == 2) {
			double s2 = 0;
			for (int i = 1; i <= n; i++) {
				s2 = s2 + 2 * this.gammaFunc(i, t, e) * i * i;
			}

			gamma = 1.3221 * Math.pow(Math.pow((s2 / s0), 2), 0.2);

			m = (int) Math.round(Math.min(t, gamma * Math.pow(t, 0.2)));

		}

		return m;
	}

	/**
	 * kernel=1: Bartllet. kernel=2: Quadratic Spectral
	 */
	private double varEstimate(double[] e, int n, int kernel, int bandwidth) {

		double var = this.gammaFunc(0, n, e);

		if (bandwidth != 0) {
			if (kernel == 1) {
				double sum = 0;
				for (int j = 1; j <= bandwidth; j++) {
					sum = sum + 2 * this.gammaFunc(j, n, e) * (1.0 - j / (bandwidth + 1.0));
				}
				var = var + sum;
			}

			if (kernel == 2) {
				double sum = 0;
				for (int j = 1; j < n; j++) {
					double a = (25 / (12 * Math.PI * Math.PI * Math.pow(((double) j / bandwidth), 2)));
					double b = (Math.sin(6 * Math.PI * ((double) j / bandwidth) / 5) / (
						6 * Math.PI * ((double) j / bandwidth) / 5)) - Math.cos(
						6 * Math.PI * ((double) j / bandwidth) / 5);
					sum = sum + 2 * this.gammaFunc(j, n, e) * a * b;
				}
				var = var + sum;
			}
		}

		return var;
	}

	/**
	 * data is the input data to be tested
	 *
	 * nLag is the number of lags to be used as bandwidth.
	 * 1: 12*(n/100)^{1/4}.(Schwert, 1989) 2: Automatic selection algorithm.(Hobijn B, Franses PH and Ooms M,
	 * 2004)
	 *
	 * kernel is the kernel function used as weight in the estimation of variance in function of alternative
	 * hypothesis.
	 * 1: bartlett.(Kwiatkowski, D., 1992) 2: Quadratic Spectral(Andrews, 1991)
	 *
	 * result contains the order for testing and two types of testing results.
	 * First type: constant only. second type: constant and trend.
	 * In each type, first element is value of statistics, second is critical value of 0.05, third is test's
	 * result.
	 *
	 *
	 * Reference:
	 * Hobijn B, Franses PH and Ooms M (2004). Generalization of the KPSS-test for stationarity. Statistica
	 * Neerlandica, vol. 58, p. 482-502.
	 * Kwiatkowski, D.; Phillips, P. C. B.; Schmidt, P.; Shin, Y. (1992). Testing the null hypothesis of
	 * stationarity against the alternative of a unit root. Journal of Econometrics, 54 (1-3): 159-178.
	 */
	void kpssTest(double[] data, int nLag, int kernel) {
		//Step 1: Sum of square under null hypothesis
		int t = data.length;

		//constant only
		double[] e1 = new double[t];
		double etc1 = 0;
		double mu = TsMethod.mean(data);
		double sume1 = 0;
		for (int i = 0; i < t; i++) {
			e1[i] = data[i] - mu;
			sume1 = sume1 + e1[i];
			etc1 = etc1 + sume1 * sume1;
		}
		etc1 = etc1 / (t * t);

		//constant and trend
		OLSMultipleLinearRegression lm = new OLSMultipleLinearRegression();
		double[][] x = new double[t][1];
		for (int i = 0; i < t; i++) {
			x[i][0] = i + 1;
		}
		lm.newSampleData(data, x);
		double[] e2 = lm.estimateResiduals();
		double etc2 = 0;
		double sume2 = 0;
		for (int i = 0; i < t; i++) {
			sume2 = sume2 + e2[i];
			etc2 = etc2 + sume2 * sume2;
		}
		etc2 = etc2 / (t * t);

		//Step 2: choose nLag
		if (nLag == 1) {
			this.cnBandwidth = Math.min((int) Math.ceil(12 * Math.pow((double) data.length / 100, 0.25)), t-1);
			this.ctBandwidth = Math.min((int) Math.ceil(12 * Math.pow((double) data.length / 100, 0.25)), t-1);
		}
		if (nLag == 2) {
			this.cnBandwidth = autoBandwidth(e1, t, kernel);
			this.ctBandwidth = autoBandwidth(e2, t, kernel);
		}

		//Step 3: estimate variance under alternative hypothesis
		//constant only
		double var1 = varEstimate(e1, t, kernel, cnBandwidth);
		//constant and trend
		double var2 = varEstimate(e2, t, kernel, ctBandwidth);

		//Step 4: compute statistics

		this.cnValue = etc1 / var1;
		this.cnCritic = 0.463;
		if (this.cnValue >= cnCritic) {
			this.cnResult = "Reject null hypothesis that the series is stationary.";
		} else {
			this.cnResult = "Do not reject null hypothesis that the series is stationary.";
		}

		this.ctValue = etc2 / var2;
		this.ctCritic = 0.146;
		if (this.ctValue >= ctCritic) {
			this.ctResult = "Reject null hypothesis that the series is stationary.";
		} else {
			this.ctResult = "Do not reject null hypothesis that the series is stationary.";
		}
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("cnValue: " + cnValue + "\n")
			.append("ctValue: " + ctValue + "\n")
			.append("cnResult " + this.cnResult + "\n")
			.append("ctResult " + this.ctResult + "\n")
			;
		return sbd.toString();
	}
}
