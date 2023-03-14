package com.alibaba.alink.operator.common.timeseries.arimagarch;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.arma.CSSGradientTarget;
import com.alibaba.alink.operator.common.timeseries.garch.GarchGradientTarget;

import java.util.ArrayList;

public class ArimaGarchGradientTarget extends AbstractGradientTarget {
	public double[] data;
	public double[] cResidual;
	public int p;
	public int q;
	public int ifIntercept;
	public int type;
	public double loglike;
	public double[] iterResidual;
	public int aOrder;
	public int bOrder;
	public double[] ch;

	public ArimaGarchGradientTarget() {
	}

	/**
	 * order of coef:  ar,ma,intercept,alpha,beta,c
	 */
	public void fit(ArrayList <double[]> init, double[] data, double[] cResidual, double[] ch, int ifIntercept) {
		this.data = data.clone();
		this.cResidual = cResidual.clone();
		this.ch = ch.clone();
		this.ifIntercept = ifIntercept;

		double[] arCoef = init.get(0).clone();
		double[] maCoef = init.get(1).clone();
		double intercept = init.get(2)[0];
		double[] alpha = init.get(3).clone();
		double[] beta = init.get(4).clone();
		double c = init.get(5)[0];
		this.p = arCoef.length;
		this.q = maCoef.length;
		this.aOrder = alpha.length;
		this.bOrder = beta.length;

		double[][] a = new double[data.length][1];
		for (int q = 0; q < data.length; q++) {
			a[q][0] = data[q];
		}
		super.setX(new DenseMatrix(a));

		double[][] m = new double[][] {};
		if (ifIntercept == 0) {
			m = new double[p + q + aOrder + bOrder + 1][1];
			for (int i = 0; i < p; i++) {
				m[i][0] = arCoef[i];
			}
			for (int i = 0; i < q; i++) {
				m[i + p][0] = maCoef[i];
			}
			for (int i = 0; i < aOrder; i++) {
				m[i + p + q][0] = alpha[i];
			}
			for (int i = 0; i < bOrder; i++) {
				m[i + p + q + aOrder][0] = beta[i];
			}
			m[p + q + aOrder + bOrder][0] = c;
		} else {
			m = new double[p + q + aOrder + bOrder + 2][1];
			for (int i = 0; i < p; i++) {
				m[i][0] = arCoef[i];
			}
			for (int i = 0; i < q; i++) {
				m[i + p][0] = maCoef[i];
			}
			m[p + q][0] = intercept;
			for (int i = 0; i < aOrder; i++) {
				m[i + p + q + 1][0] = alpha[i];
			}
			for (int i = 0; i < bOrder; i++) {
				m[i + p + q + 1 + aOrder][0] = beta[i];
			}
			m[p + q + 1 + aOrder + bOrder][0] = c;
		}

		if (m.length == 0) {
			super.initCoef = DenseMatrix.zeros(0, 0);
		} else {
			super.initCoef = new DenseMatrix(m);
		}
	}

	public ArrayList <double[]> findSeries(double[] data, ArrayList <double[]> coef) {
		double[] arCoef = coef.get(0);
		double[] maCoef = coef.get(1);
		double intercept = coef.get(2)[0];
		double[] alpha = coef.get(3);
		double[] beta = coef.get(4);
		double c = coef.get(5)[0];

		int arimainit = p;

		CSSGradientTarget cssProblem = new CSSGradientTarget();
		cssProblem.computeRSS(data, cResidual, 0, arCoef, maCoef, intercept, 1, ifIntercept);
		double[] z = new double[cssProblem.iterResidual.length - arimainit];
		for (int i = 0; i < z.length; i++) {
			z[i] = cssProblem.iterResidual[i + arimainit];
		}
		GarchGradientTarget garchProblem = new GarchGradientTarget();
		double[] h = garchProblem.computeHat(z, ch, alpha, beta, c);

		ArrayList <double[]> result = new ArrayList <>();
		result.add(z);
		result.add(h);
		result.add(cssProblem.iterResidual);
		return result;
	}

	public double computeMLE(double[] data, ArrayList <double[]> coef) {
		ArrayList <double[]> series = this.findSeries(data, coef);
		double[] z = series.get(0);
		double[] h = series.get(1);

		int garchInit = bOrder;

		double n = z.length - garchInit;
		double loglike = -(n / 2) * Math.log(2 * Math.PI);
		for (int i = garchInit; i < z.length; i++) {
			loglike = loglike - 0.5 * Math.log(h[i]);
			loglike = loglike - 0.5 * (z[i] * z[i]) / h[i];
		}

		return -loglike;
	}

	public double pComputeMLE(int t, int params, double[] data, ArrayList <double[]> coef) {
		/**
		 * params: 1(sARCoef), 2(maCoef), 3(intercept), 4(alpha), 5(beta), 6(c)
		 */

		double[] alpha = coef.get(3);
		double[] beta = coef.get(4);
		double c = coef.get(5)[0];

		ArrayList <double[]> series = this.findSeries(data, coef);
		double[] z = series.get(0);
		double[] h = series.get(1);
		double[] allZ = series.get(2);
		int arimaInit = p;
		int garchInit = bOrder;

		double gradient = 0;
		if (params == 1) {
			for (int i = garchInit; i < z.length; i++) {
				gradient = gradient - z[i] * data[i + arimaInit - t] / h[i];
			}
		}
		if (params == 2) {
			for (int i = garchInit; i < z.length; i++) {
				if (i + arimaInit - t < 0) {
					gradient = gradient - 0;
				} else {
					gradient = gradient - z[i] * allZ[i + arimaInit - t] / h[i];
				}
			}
		}
		if (params == 3) {
			for (int i = garchInit; i < z.length; i++) {
				gradient = gradient - z[i] / h[i];
			}
		}
		if (params == 4) {
			for (int i = garchInit; i < z.length; i++) {
				double sum = (1 / h[i]) - ((z[i] * z[i]) / (h[i] * h[i]));
				if (i - t < 0) {
					gradient = gradient + (sum) * alpha[t - 1] * ch[0];
				} else {
					gradient = gradient + (sum) * alpha[t - 1] * h[i - t];
				}
			}
		}
		if (params == 5) {
			for (int i = garchInit; i < z.length; i++) {
				double sum = (1 / h[i]) - ((z[i] * z[i]) / (h[i] * h[i]));
				gradient = gradient + (sum) * beta[t - 1] * z[i - t] * z[i - t];
			}
		}
		if (params == 6) {
			for (int i = garchInit; i < z.length; i++) {
				double sum = (1 / h[i]) - ((z[i] * z[i]) / (h[i] * h[i]));
				gradient = gradient + (sum) * c;
			}
		}
		return gradient;
	}

	public ArrayList <double[]> trans(DenseMatrix coef) {
		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		for (int i = 0; i < p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < q; i++) {
			maCoef[i] = coef.get(i + p, 0);
		}

		double[] alpha = new double[aOrder];
		double[] beta = new double[bOrder];
		double intercept = 0;
		if (ifIntercept == 1) {
			intercept = coef.get(p + q, 0);
		}

		for (int i = 0; i < aOrder; i++) {
			alpha[i] = coef.get(i + p + q + ifIntercept, 0);
		}
		for (int i = 0; i < bOrder; i++) {
			beta[i] = coef.get(i + p + q + aOrder + ifIntercept, 0);
		}
		double c = coef.get(p + q + aOrder + bOrder + ifIntercept, 0);

		ArrayList <double[]> result = new ArrayList <>();
		result.add(arCoef);
		result.add(maCoef);
		result.add(new double[] {intercept});
		result.add(alpha);
		result.add(beta);
		result.add(new double[] {c});

		return result;
	}

	@Override
	public DenseMatrix gradient(DenseMatrix coef, int iter) {
		ArrayList <double[]> coeff = this.trans(coef);

		double[][] a = new double[][] {};
		if (ifIntercept == 1) {
			a = new double[p + q + 2 + aOrder + bOrder][1];
		} else {
			a = new double[p + q + 1 + aOrder + bOrder][1];
		}

		for (int i = 0; i < p; i++) {
			a[i][0] = this.pComputeMLE(i + 1, 1, data, coeff);
		}
		for (int i = 0; i < q; i++) {
			a[i + p][0] = this.pComputeMLE(i + 1, 2, data, coeff);
		}
		if (ifIntercept == 1) {
			a[p + q][0] = this.pComputeMLE(1, 3, data, coeff);
		}
		for (int i = 0; i < aOrder; i++) {
			a[i + p + q + ifIntercept][0] = this.pComputeMLE(i + 1, 4, data, coeff);
		}
		for (int i = 0; i < bOrder; i++) {
			a[i + p + q + ifIntercept + aOrder][0] = this.pComputeMLE(i + 1, 5, data, coeff);
		}
		a[p + q + ifIntercept + aOrder + bOrder][0] = this.pComputeMLE(1, 6, data, coeff);

		return new DenseMatrix(a);
	}

	@Override
	public double f(DenseMatrix coef) {
		ArrayList <double[]> coeff = this.trans(coef);
		double result = this.computeMLE(this.data, coeff);
		return result;
	}
}
