package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

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
public class CSSEstimate extends ArmaEstimate {

	private int optOrder;

	public CSSEstimate() {
		warn = new ArrayList <>();
	}

	@Override
	public void compute(double[] data, int p, int q, int ifIntercept) {
		this.mean = TsMethod.mean(data);
		this.varianceStdError = 0;
		bfgsEstimateParams(data, p, q, ifIntercept);
	}

	/**
	 * Use a high level AR model to estimate residual
	 * Then regress data on data and residual to set initial parameters for computation of CSS
	 * Calculate the CSS parameters for AR and MA parts
	 *
	 * type is a constrain for grandienttarget class. "type=1" informs that the gradient is for EstimateParams
	 * Otherwise, grandienttarget is used for HannanRissanen.
	 */
	public void bfgsEstimateParams(double[] data, int p, int q, int ifIntercept) {
		int type = 1;
		//step 1: set residual[i]=0, if i<initPoint; set data[i]=0, if i<initPoint
		double[] residualOne = new double[data.length];

		//step 2: find initial parameters for fitting CSS
		double[] newARCoef = new double[p];
		double[] newMACoef = new double[q];
		double[] newIntercept = new double[1];
		ArrayList <double[]> init = new ArrayList <>();
		init.add(newARCoef);
		init.add(newMACoef);
		init.add(newIntercept);

		//step 3: get recursive CSS estimation of parameters
		CSSGradientTarget cssProblem = new CSSGradientTarget();
		cssProblem.fit(init, data, residualOne, this.optOrder, type, ifIntercept);
		AbstractGradientTarget problem = BFGS.solve(cssProblem, 10000,
			0.01, 0.000001, new int[] {1, 2, 3}, -1);

		this.residual = problem.getResidual();
		this.variance = data.length == p + q + ifIntercept ? 1.0
			: problem.getMinValue() / (data.length - p - q - ifIntercept);

		DenseMatrix result = problem.getFinalCoef();
		this.warn = problem.getWarn();
		this.arCoef = new double[p];
		this.maCoef = new double[q];
		for (int i = 0; i < p; i++) {
			this.arCoef[i] = result.get(i, 0);
		}
		for (int i = 0; i < q; i++) {
			this.maCoef[i] = result.get(i + p, 0);
		}

		if (ifIntercept == 1) {
			this.intercept = result.get(p + q, 0);
		} else {
			this.intercept = 0;
		}
		this.css = problem.getMinValue();
		this.logLikelihood = -((double) data.length / 2.0) * Math.log(2 * Math.PI)
			- ((double) data.length / 2) * Math.log(variance)
			- css / (2 * variance);
		DenseMatrix information = problem.getH();

		this.arCoefStdError = new double[p];
		this.maCoefStdError = new double[q];
		for (int i = 0; i < p; i++) {
			this.arCoefStdError[i] = Math.sqrt(information.get(i, i));
			if (Double.isNaN(this.arCoefStdError[i])) {
				this.arCoefStdError[i] = -99;
			}
		}
		for (int i = 0; i < q; i++) {
			this.maCoefStdError[i] = Math.sqrt(information.get(i + p, i + p));
			if (Double.isNaN(this.maCoefStdError[i])) {
				this.maCoefStdError[i] = -99;
			}
		}
		if (ifIntercept == 1) {
			this.interceptStdError = Math.sqrt(information.get(p + q, p + q));
			if (Double.isNaN(this.interceptStdError)) {
				this.interceptStdError = -99;
			}
		} else {
			this.interceptStdError = 0;
		}

	}

}
