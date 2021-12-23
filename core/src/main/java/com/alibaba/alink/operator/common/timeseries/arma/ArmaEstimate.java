package com.alibaba.alink.operator.common.timeseries.arma;

import java.util.ArrayList;

public abstract class ArmaEstimate {

	public double[] arCoef;
	public double[] maCoef;
	public double intercept;
	public double css;
	public double mean;
	public double variance;
	public double logLikelihood;
	public double[] residual;

	public double[] arCoefStdError;
	public double[] maCoefStdError;
	public double interceptStdError;
	public double varianceStdError;

	public ArrayList <String> warn;

	public abstract void compute(double[] data, int p, int q, int ifIntercept);

	/**
	 * check for causal and invertible attributes
	 */
	void checkCoef() {
		boolean symbol1 = arFunc(-1.01) > 0;
		boolean symbol2 = maFunc(-1.01) > 0;
		for (double i = -1.01; i <= 1.01; i = i + 0.01) {
			double y1 = arFunc(i);
			if ((y1 > 0) != symbol1) {
				if (warn == null) {
					warn = new ArrayList <String>();
				}
				this.warn.add("2");
				break;
			}
		}

		for (double i = -1.01; i <= 1.01; i = i + 0.01) {
			double y2 = maFunc(i);
			if ((y2 > 0) != symbol2) {
				if (warn == null) {
					warn = new ArrayList <String>();
				}
				this.warn.add("3");
				break;
			}
		}

	}

	private double arFunc(double x) {
		double result = 1;
		for (int i = 0; i < this.arCoef.length; i++) {
			result = result - Math.pow(x, i + 1) * arCoef[i];
		}
		return result;
	}

	private double maFunc(double x) {
		double result = 1;
		for (int i = 0; i < this.maCoef.length; i++) {
			result = result + Math.pow(x, i + 1) * maCoef[i];
		}
		return result;
	}
}
