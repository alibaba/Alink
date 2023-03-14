package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.util.ArrayList;

/**
 * SCSSEstimate is a class to compute parameters of ARMA model using CSS method.
 * It is used as first step for CSS-MLE or MLE method as well.
 * <p>
 * EstimateParams use gradient descent method to solve conditional sum of square of the model.
 * The initial parameters for GD is set by regressing data on data and residual and taking coefficients.
 * The result could be regarded as the initial parameters for "CSS-MLE" method.
 * <p>
 * HannanRissanen use gradient descent method to solve conditional sum of square of the model.
 * The initial parameters for GD is set by regressing data on data and residual and taking coefficients.
 * The result could be regarded as the initial parameters for "MLE" method.
 * <p>
 * <p>
 * Warning: for this method,the number of sample minus max(p,q) should be Greater than p+q,
 * namely, data.length - Math.max(sARCoef.length, maCoef.length) > sARCoef.length + maCoef.length
 */
public class HrEstimate extends ArmaEstimate {

	private int optOrder;

	public HrEstimate() {
		warn = new ArrayList <>();
	}

	@Override
	public void compute(double[] data, int p, int q, int ifIntercept) {
		hannanRissanen(data, p, q, ifIntercept);
	}

	/**
	 * get start parameters for method "MLE"
	 *
	 * Reference:
	 * E. J. Hannan and J. Rissanen (1982): Recursive Estimation of Mixed Autoregressive-Moving Average Order.
	 * Biometrika 69, 81--94.
	 */
	private void hannanRissanen(double[] data, int p, int q, int ifIntercept) {
		int type = 0;
		//step 1: get residual simulated by high order AR model
		double[] residualOne = errorTerm(data);

		//step 2: get CSS estimation of parameters
		ArrayList <double[]> init = initParam(data, residualOne, p, q,
			this.optOrder, this.mean, ifIntercept);
		CSSGradientTarget cssProblem = new CSSGradientTarget();
		cssProblem.fit(init, data, residualOne, this.optOrder, type, ifIntercept);
		AbstractGradientTarget problem = BFGS.solve(cssProblem, 10000, 0.01,
			0.000001, new int[] {1, 2, 3}, -1);

		this.residual = problem.getResidual();
		this.css = problem.getMinValue();

		DenseMatrix result = problem.getFinalCoef();
		double[] bestARCoef = new double[p];
		double[] bestMACoef = new double[q];
		for (int i = 0; i < p; i++) {
			bestARCoef[i] = result.get(i, 0);
		}
		for (int i = 0; i < q; i++) {
			bestMACoef[i] = result.get(i + p, 0);
		}

		double bestIntercept;
		if (ifIntercept == 1) {
			bestIntercept = result.get(p + q, 0);
		} else {
			bestIntercept = 0;
		}

		ArrayList <double[]> resultTwo = new ArrayList <double[]>();
		resultTwo.add(bestARCoef);
		resultTwo.add(bestMACoef);
		resultTwo.add(new double[] {bestIntercept});
		//step 3: get approximated MLE estimation of parameters
		approxMLE(data, resultTwo, 10, 0.01);
	}

	/**
	 * Initialize parameters by fitting a linear regression on data and residual.
	 */
	public static ArrayList <double[]> initParam(double[] data, double[] residual,
												 int p, int q, int optOrder, double mean,
												 int ifIntercept) {

		//regression
		OLSMultipleLinearRegression lm = new OLSMultipleLinearRegression();
		//construct data set
		int len = p + q;
		int start = Math.max(p, q) + optOrder;
		double[] y = new double[data.length - start];
		double[][] x = new double[data.length - start][p + q];
		for (int i = start; i < data.length; i++) {
			y[i - start] = data[i];
			for (int j = 0; j < len; j++) {
				if (j < p) {
					x[i - start][j] = data[i - j - 1];
				}
				if (j >= p) {
					x[i - start][j] = residual[i - j + p - 1];
				}
			}
		}

		lm.newSampleData(y, x);

		//Compute approximated MLE parameters of this iteration
		//first of Beta is intercept
		double[] beta = lm.estimateRegressionParameters();

		double[] newARCoef = new double[p];
		double[] newMACoef = new double[q];
		double[] newIntercept = new double[1];

		System.arraycopy(beta, 1, newARCoef, 0, p);
		for (int i = 0; i < q; i++) {
			newMACoef[i] = beta[p + i + 1];
		}
		double sum = 1;
		for (int i = 0; i < p; i++) {
			sum = sum - newARCoef[i];
		}
		if (ifIntercept == 1) {
			newIntercept[0] = beta[0];
		} else {
			newIntercept[0] = 0;
		}

		ArrayList <double[]> result = new ArrayList <>();
		result.add(newARCoef);
		result.add(newMACoef);
		result.add(newIntercept);
		return result;
	}

	/**
	 * First step: construct a high order AR model to estimate residuals
	 * Use AIC criteria to select order 'optOrder'.
	 * Compute residuals of AR model according to data.
	 *
	 * 'residual' contains (data.length-optOrder) number of residual, starting from optOrder+1 to data.length.
	 * 'residual1' contains (data.length) number of residual, starting from 1 to data.length.
	 */
	private double[] errorTerm(double[] data) {
		double[] data1 = new double[data.length];
		this.mean = TsMethod.mean(data);
		for (int i = 0; i < data.length; i++) {
			data1[i] = data[i] - mean;
		}
		int n = (int) Math.round(1.1 * Math.log(data.length));

		//find best order for AR model, used to compute residual
		double[][] coef = TsMethod.levinson(TsMethod.acvf(data1, n));
		double minAIC = Double.POSITIVE_INFINITY;
		int optOrder = 0;
		for (int m = 0; m < coef[0].length; m++) {
			double aic = Math.log(coef[0][m]) + m * 2 / data1.length;
			if (aic < minAIC) {
				minAIC = aic;
				optOrder = m;
			}
		}

		//AR coefficients
		double[] optCoef = new double[optOrder];
		System.arraycopy(coef[optOrder], 1, optCoef, 0, optCoef.length);

		//Compute residual
		double[] residual = new double[data.length - optOrder];
		for (int i = 0; i < residual.length; i++) {
			residual[i] = data1[i + optOrder];
			for (int h = 0; h < optOrder; h++) {
				residual[i] = residual[i] - optCoef[h] * data1[i + optOrder - h - 1];
			}
		}

		//Another formation
		double[] residual1 = new double[data1.length];
		System.arraycopy(residual, 0, residual1, optOrder, residual1.length - optOrder);

		this.optOrder = optOrder;

		return residual1;
	}

	/**
	 * For returned ArrayList:
	 * first index is AR parameters
	 * second index is MA parameters
	 * third index is variance of this iteration
	 */
	private ArrayList <double[]> iterParam(double[] data, double[] arCoef, double[] maCoef, double intercept) {
		//Create residual and two part for regression
		double[] residual = new double[data.length];
		double[] a = new double[data.length];
		double[] b = new double[data.length];
		int start = Math.max(arCoef.length, maCoef.length);

		for (int i = start; i < residual.length; i++) {
			//Compute residual on lag i
			residual[i] = new CSSGradientTarget().oneRSS(i, data, residual, arCoef, maCoef, intercept);
			//Compute AR part on lag i
			for (int j = 1; j <= arCoef.length; j++) {
				a[i] = a[i] + arCoef[j - 1] * a[i - j];
			}
			a[i] = -a[i] + residual[i];

			//Compute MA part on lag i
			for (int j = 1; j < maCoef.length; j++) {
				b[i] = b[i] + maCoef[j - 1] * b[i - j];
			}
			b[i] = -b[i] + residual[i];
		}

		//regression
		OLSMultipleLinearRegression lm = new OLSMultipleLinearRegression();
		//construct data set
		double[] y = new double[data.length - start];
		double[][] x = new double[data.length - start][arCoef.length + maCoef.length];
		for (int i = start; i < data.length; i++) {
			y[i - start] = residual[start];
			for (int j = 0; j < arCoef.length + maCoef.length; j++) {
				if (j < arCoef.length) {
					x[i - start][j] = -a[i - j - 1];
				}
				if (j >= arCoef.length) {
					x[i - start][j] = b[i - j + arCoef.length - 1];
				}
			}
		}

		lm.newSampleData(y, x);

		//Compute approximated MLE parameters of this iteration
		//first of Beta is intercept
		double[] beta = lm.estimateRegressionParameters();
		double[] newARCoef = new double[arCoef.length];
		double[] newMACoef = new double[maCoef.length];
		for (int i = 0; i < newARCoef.length; i++) {
			newARCoef[i] = arCoef[i] + beta[i + 1];
		}
		for (int i = 0; i < newMACoef.length; i++) {
			newMACoef[i] = maCoef[i] + beta[arCoef.length + i + 1];
		}

		//Compute n*variance
		double[] r = new double[data.length];
		double rss = 0;
		for (int i = start; i < r.length; i++) {
			//Compute residual on lag i
			double res = new CSSGradientTarget().oneRSS(i, data, r, newARCoef, newMACoef, intercept);
			r[i] = res;
			rss = rss + res * res;
		}

		ArrayList <double[]> result = new ArrayList <>();
		result.add(newARCoef);
		result.add(newMACoef);
		result.add(new double[] {rss / (data.length - start)});
		return result;
	}

	private void approxMLE(double[] data, ArrayList <double[]> coef, int maxIter, double threshold) {
		double[] arCoef = coef.get(0);
		double[] maCoef = coef.get(1);
		double intercept = coef.get(2)[0];
		double oldSigma2 = Double.MAX_VALUE;
		double sigma2 = 0;

		for (int i = 0; i < maxIter; i++) {
			ArrayList <double[]> iresult = this.iterParam(data, arCoef, maCoef, intercept);
			arCoef = iresult.get(0);
			maCoef = iresult.get(1);
			sigma2 = iresult.get(2)[0];

			if (Math.abs(sigma2 - oldSigma2) <= threshold) {
				break;
			}

			oldSigma2 = sigma2;
		}
		this.arCoef = arCoef;
		this.maCoef = maCoef;
		this.intercept = intercept;
		this.variance = sigma2;
		this.logLikelihood = Math.log(this.variance);
	}

}
