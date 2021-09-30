package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;

import java.util.ArrayList;

public class CSSMLEEstimate extends ArmaEstimate {

	public ArrayList <String> warn;

	public CSSMLEEstimate() {
	}

	@Override
	public void compute(double[] data, int p, int q, int ifIntercept) {
		this.mean = TsMethod.mean(data);
		if (p == 0 && q == 0) {
			this.arCoef = new double[] {};
			this.maCoef = new double[] {};
			this.arCoefStdError = new double[] {};
			this.maCoefStdError = new double[] {};
			if (ifIntercept == 0) {
				double ss = 0;
				for (double aData : data) {
					ss += aData * aData;
				}
				this.variance = ss / (data.length - 1);
				this.css = ss;
				this.intercept = 0;
			} else {
				this.variance = TsMethod.computeACVF(data, 0)[0];
				this.css = this.variance * data.length;
				this.intercept = this.mean;
			}
			this.logLikelihood = -data.length / 2.0 * Math.log(2 * Math.PI)
				- data.length / 2.0 * Math.log(variance)
				- css / (2.0 * variance);
			this.residual = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				this.residual[i] = data[i] - intercept;
			}
		} else {
			this.css = 0;
			bfgsEstimateParams(data, p, q, ifIntercept);
		}
	}

	private void bfgsEstimateParams(double[] data, int p, int q, int ifIntercept) {
		//step 1: find CSS result
		ArmaModel cssARMA = new ArmaModel(p, q, EstMethod.Css, ifIntercept);
		cssARMA.fit(data);
		ArrayList <double[]> init = new ArrayList <>();
		init.add(cssARMA.estimate.arCoef.clone());
		init.add(cssARMA.estimate.maCoef.clone());
		init.add(new double[] {cssARMA.estimate.intercept});
		init.add(new double[] {cssARMA.estimate.variance});

		//step 2: find MLE result via BFGS
		//set residual[i]=0, if i<MA order
		double[] residualOne = new double[data.length];

		MLEGradientTarget mleProblem = new MLEGradientTarget();
		mleProblem.fit(init, data, residualOne, ifIntercept);

		int varianceIdx = cssARMA.estimate.arCoef.length + cssARMA.estimate.maCoef.length;
		AbstractGradientTarget problem = BFGS.solve(mleProblem, 1000,
			0.01, 0.000001, new int[] {1, 2, 3}, varianceIdx);

		this.residual = problem.getResidual();
		this.logLikelihood = problem.getMinValue();

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
			this.variance = result.get(p + q + 1, 0);
		} else {
			this.variance = result.get(p + q, 0);
			this.intercept = 0;
		}
		this.logLikelihood = -problem.getMinValue();

		if (problem.getIter() < 2) {
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
			this.varianceStdError = Math.sqrt(information.get(p + q, p + q));
			if (Double.isNaN(this.varianceStdError)) {
				this.varianceStdError = -99;
			}
			if (ifIntercept == 1) {
				this.interceptStdError = Math.sqrt(information.get(p + q + 1, p + q + 1));
				if (Double.isNaN(this.interceptStdError)) {
					this.interceptStdError = -99;
				}
			} else {
				this.interceptStdError = 0;
			}
		} else {
			this.arCoefStdError = cssARMA.estimate.arCoefStdError;
			this.maCoefStdError = cssARMA.estimate.maCoefStdError;
			this.interceptStdError = cssARMA.estimate.interceptStdError;
			this.varianceStdError = 1;
		}

	}

}
