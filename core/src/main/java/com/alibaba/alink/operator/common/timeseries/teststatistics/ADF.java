package com.alibaba.alink.operator.common.timeseries.teststatistics;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class ADF {
	public int lag;
	public double nnValue;
	public double cnValue;
	public double ctValue;
	public double nCritic;
	public double ctCritic;
	public String nnResult;
	public String cnResult;
	public String ctResult;

	/**
	 * data is the data to be tested
	 * maxLag is the maximum number of lags to be considered in nLag.
	 * nLag is the choice of lag for testing. 1: use 12*(n/100)^{1/4} lag. 2: use AIC. 3: use BIC.
	 * Reference for AIC and BIC, “Lag Length Selection and the Construction of Unit Root Tests with Good Size and
	 * Power,” ECTA, 2001.
	 *
	 * result contains the order for testing and three types of testing results.
	 * First type: no trend, no constant. Second type: constant only. Third type: constant and trend.
	 * In each type, first element is value of adf, second is critical value of 0.05, third is test's result.
	 */
	void adfTest(double[] data, int maxLag, int nLag) {
		if (maxLag >= data.length) {
			throw new RuntimeException("maxLag must be smaller than rows of data");
		}
		if (nLag != 1 && nLag != 2 && nLag != 3) {
			throw new RuntimeException("nLag must be 1: use 12*(n/100)^{1/4} lag. 2: use AIC. 3: use BIC.");
		}

		double[] dData = new StationaryTest().difference(data);
		int order = 0;

		//order selection
		if (nLag == 1) {
			order = (int) Math.floor(12 * Math.pow((double) data.length / 100, 0.25));
			if (order > maxLag) {
				order = maxLag;
			}
		}
		//use information criteria
		else {
			double bestIC = Double.MAX_VALUE;
			for (int q = 0; q <= maxLag; q++) {
				double[] yAll = new double[data.length - q - 1];
				double[][] xAll = new double[data.length - q - 1][q + 1];
				for (int i = q + 1; i < data.length; i++) {
					yAll[i - q - 1] = dData[i];
					xAll[i - q - 1][0] = data[i - 1];
					for (int j = 0; j < q; j++) {
						xAll[i - q - 1][j + 1] = dData[i - j - 1];
					}
				}
				OLSMultipleLinearRegression test = new OLSMultipleLinearRegression();
				test.newSampleData(yAll, xAll);
				double sigma = test.calculateResidualSumOfSquares() / (data.length - maxLag);
				double beta = test.estimateRegressionParameters()[1];
				double sum = 0;
				for (int p = maxLag; p < data.length; p++) {
					sum = sum + data[p - 1] * data[p - 1];
				}
				double t = beta * beta * sum / sigma;

				double ic = 0;
				//AIC
				if (nLag == 2) {
					ic = Math.log(sigma) + 2 * (t + q) / (data.length - maxLag);
				}
				//BIC
				if (nLag == 3) {
					ic = Math.log(sigma) + Math.log(data.length - maxLag) * (t + q) / (data.length - maxLag);
				}

				if (ic < bestIC) {
					bestIC = ic;
					order = q;
				}
			}
		}

		this.lag = order;

		double[] y = new double[data.length - order - 1];
		double[][] x1 = new double[data.length - order - 1][order + 1];
		//include trend
		double[][] x2 = new double[data.length - order - 1][order + 2];
		//construct y and y for regression
		for (int i = order + 1; i < data.length; i++) {
			y[i - order - 1] = dData[i];
			x1[i - order - 1][0] = data[i - 1];
			x2[i - order - 1][0] = data[i - 1];
			x2[i - order - 1][1] = i;
			for (int j = 0; j < order; j++) {
				x1[i - order - 1][j + 1] = dData[i - j - 1];
				x2[i - order - 1][j + 2] = dData[i - j - 1];
			}
		}

		//select critical value
		//6 rows: n=25,50,100,250,500,infinity
		//4 columns: no trend 0.01, no trend 0.05, trend 0.01, trend 0.05
		double[][] critic = {{-3.75, -3.00, -4.38, -3.60}, {-3.58, -2.93, -4.15, -3.50}, {-3.51, -2.89, -4.04, -3.45},
			{-3.46, -2.88, -3.99, -3.43}, {-3.44, -2.87, -3.98, -3.42}, {-3.43, -2.86, -3.96, -3.41}};
		int row = 0;
		if (dData.length <= 25) {
			row = 0;
		} else if (dData.length <= 50) {
			row = 1;
		} else if (dData.length <= 100) {
			row = 2;
		} else if (dData.length <= 250) {
			row = 3;
		} else if (dData.length <= 500) {
			row = 5;
		} else {
			row = 6;
		}

		double criticNT = critic[row][1];
		double criticT = critic[row][3];

		OLSMultipleLinearRegression lm = new OLSMultipleLinearRegression();

		//result with out constant and trend
		lm.setNoIntercept(true);
		lm.newSampleData(y, x1);

		double tao = lm.estimateRegressionParameters()[0] / lm.estimateRegressionParametersStandardErrors()[0];
		this.nnValue = tao;
		this.nCritic = criticNT;
		if (tao < criticNT) {
			this.nnResult = "Reject null hypothesis that the series is not stationary.";
		} else {
			this.nnResult = "Do not reject null hypothesis that the series is not stationary.";
		}

		//result with constant only
		lm.setNoIntercept(false);
		lm.newSampleData(y, x1);

		tao = lm.estimateRegressionParameters()[1] / lm.estimateRegressionParametersStandardErrors()[1];
		this.cnValue = tao;
		if (tao < criticNT) {
			this.cnResult = "Reject null hypothesis that the series is not stationary.";
		} else {
			this.cnResult = "Do not reject null hypothesis that the series is not stationary.";
		}

		//result with constant and trend
		lm.newSampleData(y, x2);
		tao = lm.estimateRegressionParameters()[1] / lm.estimateRegressionParametersStandardErrors()[1];
		this.ctValue = tao;
		this.ctCritic = criticT;
		if (tao < criticT) {
			this.ctResult = "Reject null hypothesis that the series is not stationary.";
		} else {
			this.ctResult = "Do not reject null hypothesis that the series is not stationary.";
		}

	}
}
