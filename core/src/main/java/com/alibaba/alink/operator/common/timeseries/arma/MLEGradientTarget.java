package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.arma.CSSGradientTarget;

import java.util.ArrayList;

public class MLEGradientTarget extends AbstractGradientTarget {
	public double[] data;
	public double[] cResidual;
	public int p;
	public int q;
	public int ifIntercept;
	public int type;
	public double logLikelihood;
	public double[] iterResidual;

	public MLEGradientTarget() {
	}

	public void fit(ArrayList <double[]> init, double[] data, double[] cResidual, int ifIntercept) {
		double[] arCoef = init.get(0);
		double[] maCoef = init.get(1);
		double intercept = init.get(2)[0];
		double variance = init.get(3)[0];
		this.p = arCoef.length;
		this.q = maCoef.length;
		this.data = data;
		this.cResidual = cResidual;
		this.type = 1;
		double[][] a = new double[data.length][1];
		for (int q = 0; q < data.length; q++) {
			a[q][0] = data[q];
		}
		super.setX(new DenseMatrix(a));

		double[][] m;
		this.ifIntercept = ifIntercept;
		if (ifIntercept == 0) {
			m = new double[arCoef.length + maCoef.length + 1][1];
			m[m.length - 1][0] = variance;
		} else {
			m = new double[arCoef.length + maCoef.length + 2][1];
			m[m.length - 1][0] = variance;
			m[m.length - 2][0] = intercept;
		}
		for (int i = 0; i < arCoef.length; i++) {
			m[i][0] = arCoef[i];
		}
		for (int i = 0; i < maCoef.length; i++) {
			m[i + arCoef.length][0] = maCoef[i];
		}
		if (m.length == 0) {
			super.initCoef = DenseMatrix.zeros(0, 0);
		} else {
			super.initCoef = new DenseMatrix(m);
		}
	}

	private double computeMLE(double[] data, double[] residual,
							  double[] arCoef, double[] maCoef, double intercept,
							  double variance, int type, int ifIntercept) {
		int initPoint = Math.max(arCoef.length, maCoef.length);

		CSSGradientTarget cssProblem = new CSSGradientTarget();

		double s = cssProblem.computeRSS(data, residual,
			0, arCoef, maCoef, intercept, type, ifIntercept);

		this.iterResidual = cssProblem.iterResidual.clone();

		//double logLikelihood = -((double) (data.length - initPoint) / 2) * Math.log(2 * Math.PI)
		//	- ((double) (data.length - initPoint) / 2) * (variance <= 0 ? 0 : Math.log(variance))
		//	- s / (2.0 * variance);
		double logLikelihood = -((data.length - initPoint) / 2.0) * Math.log(2.0 * Math.PI)
			- ((data.length - initPoint) / 2.0) * (Math.log(variance))
			- s / (2.0 * variance);

		return -logLikelihood;
	}

	/**
	 * Compute partial Inverse of MLE function.
	 * param decides whether the partial Inverse is for AR coefficients, MA coefficients or intercept. 1 for AR, 2
	 * for MA, 3 for intercept, 4 for variance.
	 * j decides which coefficient is computed for, from 0 to order-1, in AR or MA estimation.
	 */
	private double pComputeMLE(int j, double[] data, double[] cResidual, double[] arCoef, double[] maCoef,
							   double intercept, double sigma2, int param) {

		CSSGradientTarget cssProblem = new CSSGradientTarget();
		cssProblem.computeRSS(data, cResidual, 0, arCoef, maCoef, intercept, type, ifIntercept);

		double gradient = 0;
		if (param == 1) {
			double partialS = cssProblem.pComputeRSS(j, data, cssProblem.iterResidual, 0, arCoef, maCoef, intercept, 0,
				1);
			gradient = -partialS / (2.0 * sigma2);
		}
		//MA partial Inverse
		if (param == 2) {
			double partialS = cssProblem.pComputeRSS(j, data, cssProblem.iterResidual, 0, arCoef, maCoef, intercept, 0,
				2);
			gradient = -partialS / (2.0 * sigma2);
		}
		//intercept partial Inverse
		if (param == 3) {
			double partialS = cssProblem.pComputeRSS(j, data, cssProblem.iterResidual, 0, arCoef, maCoef, intercept, 0,
				3);
			gradient = -partialS / (2.0 * sigma2);
		}
		if (param == 4) {
			double s = cssProblem.computeRSS(data, cResidual, 0, arCoef, maCoef, intercept, 0, ifIntercept);
			gradient = -((double) data.length / (2.0 * sigma2)) + s / (2.0 * sigma2 * sigma2);
		}
		return -gradient;
	}

	private double[] rowHessian(int param, int i, int j, double[] data, double[] residual, double[] arCoef,
								double[] maCoef, double intercept, double sigma2, int ifIntercept) {
		double[] hess = new double[arCoef.length + maCoef.length + 1 + ifIntercept];
		double p = this.pComputeMLE(i, data, residual, arCoef, maCoef, intercept, sigma2, param);
		if (j < arCoef.length) {
			double[] newAR = arCoef.clone();
			newAR[j] += 0.00001;

			hess[j] = (this.pComputeMLE(i, data, residual, newAR, maCoef, intercept, sigma2, param) - p) / 0.00001;
		} else {
			if (j < arCoef.length + maCoef.length) {
				double[] newMA = maCoef.clone();
				newMA[j - arCoef.length] += 0.00001;
				hess[j] = (this.pComputeMLE(i, data, residual, arCoef, newMA, intercept, sigma2, param) - p) / 0.00001;
			}
		}
		if (j == arCoef.length + maCoef.length) {
			double newSigma2 = sigma2 + 0.00001;
			hess[j] = (this.pComputeMLE(i, data, residual, arCoef, maCoef, intercept, newSigma2, param) - p) / 0.00001;
		}
		if (ifIntercept == 1) {
			double newIntercept = intercept + 0.00001;
			hess[j] = (this.pComputeMLE(i, data, residual, arCoef, maCoef, newIntercept, sigma2, param) - p) / 0.00001;
		}
		return hess;
	}

	public DenseMatrix hessian(double[] data, double[] residual, double[] arCoef, double[] maCoef, double intercept,
							   double sigma2, int ifIntercept) {
		int dim = arCoef.length + maCoef.length + 1 + ifIntercept;
		double[][] hess = new double[dim][dim];
		for (int i = 0; i < hess.length; i++) {
			for (int j = 0; j < hess.length; j++) {
				if (i < arCoef.length) {
					hess[i] = rowHessian(1, i, j, data, residual, arCoef, maCoef, intercept, sigma2, ifIntercept);
				} else {
					if (i < arCoef.length + maCoef.length) {
						hess[i] = rowHessian(2, i - arCoef.length, j, data, residual, arCoef, maCoef, intercept,
							sigma2,
							ifIntercept);
					}
				}
				if (i == arCoef.length + maCoef.length) {
					hess[i] = rowHessian(4, i, j, data, residual, arCoef, maCoef, intercept, sigma2, ifIntercept);
				}
				if (ifIntercept == 1 && i == hess.length - 1) {
					hess[i] = rowHessian(3, i, j, data, residual, arCoef, maCoef, intercept, sigma2, ifIntercept);
				}
			}
		}
		return new DenseMatrix(hess);
	}

	@Override
	public DenseMatrix gradient(DenseMatrix coef, int iter) {
		if (coef.numRows() != p + q + 2 && coef.numRows() != p + q + 1) {
			throw new RuntimeException("Error!coef is not comparable with the model");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		double intercept;
		double sigma2;
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}

		if (this.ifIntercept == 1) {
			intercept = coef.get(p + q, 0);
			sigma2 = coef.get(p + q + 1, 0);
		} else {
			intercept = 0;
			sigma2 = coef.get(p + q, 0);
		}

		double[][] newGradient;
		if (ifIntercept == 1) {
			newGradient = new double[p + q + 2][1];
		} else {
			newGradient = new double[p + q + 1][1];
		}

		//gradient of AR parameters
		for (int j = 0; j < this.p; j++) {
			//partial Inverse of ComputeRSS at sARCoef[j]
			newGradient[j][0] = this.pComputeMLE(j, data, this.cResidual, arCoef, maCoef, intercept, sigma2, 1);
		}

		//gradient of MA parameters
		for (int j = 0; j < this.q; j++) {
			//partial Inverse of ComputeRSS at maCoef[j]
			newGradient[j + this.p][0] = this.pComputeMLE(j, data, this.cResidual, arCoef, maCoef, intercept, sigma2,
				2);
		}

		//gradient of intercept and variance
		if (this.ifIntercept == 1) {
			newGradient[this.p + this.q][0] = this.pComputeMLE(0, data, this.cResidual, arCoef, maCoef, intercept,
				sigma2, 3);
			newGradient[this.p + this.q + 1][0] = this.pComputeMLE(0, data, this.cResidual, arCoef, maCoef, intercept,
				sigma2, 4);
		} else {
			newGradient[this.p + this.q][0] = this.pComputeMLE(0, data, this.cResidual, arCoef, maCoef, intercept,
				sigma2, 4);
		}

		return new DenseMatrix(newGradient);
	}

	@Override
	public double f(DenseMatrix coef) {
		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		double variance;
		double intercept;
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}

		if (this.ifIntercept == 1) {
			intercept = coef.get(p + q, 0);
			variance = coef.get(p + q + 1, 0);
		} else {
			intercept = 0;
			variance = coef.get(p + q, 0);
		}
		this.logLikelihood = this.computeMLE(this.data, this.cResidual,
			arCoef, maCoef, intercept, variance, this.type,
			this.ifIntercept);
		super.residual = this.iterResidual;

		return this.logLikelihood;
	}
}
