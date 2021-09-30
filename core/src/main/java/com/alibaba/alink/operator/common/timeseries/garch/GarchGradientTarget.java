package com.alibaba.alink.operator.common.timeseries.garch;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

public class GarchGradientTarget {
	public double[] data;
	public int aOrder;
	public int bOrder;
	public double[] ch;
	public double initH;

	protected DenseMatrix x;
	protected DenseMatrix initCoef;
	protected double[] residual;

	public void fit(double[] alpha, double[] beta, double c, double[] data, double[] ch) {
		this.aOrder = alpha.length;
		this.bOrder = beta.length;
		this.data = data.clone();
		this.ch = ch.clone();
		this.initH = ch[0];
		double[][] a = new double[data.length][1];
		for (int q = 0; q < data.length; q++) {
			a[q][0] = data[q];
		}
		x = new DenseMatrix(a);

		double[][] m = new double[aOrder + bOrder + 1][1];

		for (int i = 0; i < aOrder; i++) {
			m[i][0] = alpha[i];
		}
		for (int i = 0; i < bOrder; i++) {
			m[i + aOrder][0] = beta[i];
		}
		m[aOrder + bOrder][0] = c;

		if (m.length == 0) {
			initCoef = DenseMatrix.zeros(0, 0);
		} else {
			initCoef = new DenseMatrix(m);
		}
	}

	private double onehHat(int t, double[] data, double[] h, double[] alpha, double[] beta, double c) {
		double result = c * c;

		if (t < alpha.length) {
			for (int i = 0; i < t; i++) {
				result += alpha[i] * alpha[i] * h[t - i - 1];
			}
			for (int i = t; i < alpha.length - t; i++) {
				result += alpha[i] * alpha[i] * initH;
			}
		} else {
			for (int i = 0; i < alpha.length; i++) {
				result += alpha[i] * alpha[i] * h[t - i - 1];
			}
		}

		for (int i = 0; i < beta.length; i++) {
			result += beta[i] * beta[i] * data[t - i - 1] * data[t - i - 1];
		}
		return result;
	}

	/**
	 * estimated h
	 */
	public double[] computeHat(double[] data, double[] ch, double[] alpha, double[] beta, double c) {
		int bOrder = beta.length;
		double[] hHat = ch.clone();
		for (int t = bOrder; t < data.length; t++) {
			hHat[t] = onehHat(t, data, hHat, alpha, beta, c);
		}
		return hHat;
	}

	private double computeMLE(double[] data, double[] ch, double[] alpha, double[] beta, double c) {
		int initPoint = Math.max(aOrder, bOrder);
		int n = data.length - initPoint;
		double[] hHat = computeHat(data, ch, alpha, beta, c);

		double logLikelihood = -(n / 2.0) * Math.log(2 * Math.PI);
		for (int i = initPoint; i < data.length; i++) {
			logLikelihood -= 0.5 * (Math.log(hHat[i]));
			logLikelihood -= 0.5 * (data[i] * data[i] / hHat[i]);
		}

		//return negative value for minimization
		return -logLikelihood;
	}

	/**
	 * params: 1(alpha), 2(beta), 3(c)
	 */
	private double computeMLEGradient(int t, int params, double[] data,
									  double[] ch, double[] alpha, double[] beta,
									  double c) {

		int initPoint = Math.max(aOrder, bOrder);
		double[] hHat = computeHat(data, ch, alpha, beta, c);
		double result = 0;
		//alpha
		if (params == 1) {
			for (int i = initPoint; i < data.length; i++) {
				result += ((1 / hHat[i]) - (data[i] * data[i] / (hHat[i] * hHat[i]))) * hHat[i - t] * 2 * alpha[t - 1];
			}
		}
		//beta
		if (params == 2) {
			for (int i = initPoint; i < data.length; i++) {
				result += ((1 / hHat[i]) - (data[i] * data[i] / (hHat[i] * hHat[i]))) * data[i - t] * data[i - t] * 2
					* beta[t - 1];
			}
		}
		//c
		if (params == 3) {
			for (int i = initPoint; i < data.length; i++) {
				result += ((1 / hHat[i]) - (data[i] * data[i] / (hHat[i] * hHat[i]))) * 2 * c;
			}
		}
		return result * 0.5;
	}

	public DenseVector gradient(DenseVector coef) {
		double[] alpha = new double[aOrder];
		double[] beta = new double[bOrder];
		for (int i = 0; i < aOrder; i++) {
			alpha[i] = coef.get(i);
		}
		for (int i = 0; i < bOrder; i++) {
			beta[i] = coef.get(i + aOrder);
		}
		double c = coef.get(aOrder + bOrder);

		double[] gradient = new double[aOrder + bOrder + 1];
		for (int t = 0; t < aOrder; t++) {
			gradient[t] = computeMLEGradient(t + 1, 1, data, ch, alpha, beta, c);
		}
		for (int t = 0; t < bOrder; t++) {
			gradient[t + aOrder] = computeMLEGradient(t + 1, 2, data, ch, alpha, beta, c);
		}
		gradient[aOrder + bOrder] = computeMLEGradient(0, 3, data, ch, alpha, beta, c);

		return new DenseVector(gradient);
	}

	public double f(DenseVector coef) {
		double[] alpha = new double[aOrder];
		double[] beta = new double[bOrder];
		for (int i = 0; i < aOrder; i++) {
			alpha[i] = coef.get(i);
		}
		for (int i = 0; i < bOrder; i++) {
			beta[i] = coef.get(i + aOrder);
		}
		double c = coef.get(aOrder + bOrder);

		residual = computeHat(data, ch, alpha, beta, c);
		return computeMLE(data, ch, alpha, beta, c);
	}

	public DenseVector initParams() {
		return new DenseVector(initCoef.getColumn(0));
	}
}
