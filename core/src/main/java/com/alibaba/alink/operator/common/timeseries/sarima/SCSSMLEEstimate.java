package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;

import java.util.ArrayList;

public class SCSSMLEEstimate extends SarimaEstimate {

	private double varianceStdError;

	public SCSSMLEEstimate() {
	}

	public void compute(double[][] seasonMatrix, int sP, int sQ, int season) {

		//step 1: find CSS result
		SCSSEstimate scssEstimate = new SCSSEstimate();
		scssEstimate.compute(seasonMatrix, sP, sQ, season);

		ArrayList <double[]> initCoef = new ArrayList <>();
		initCoef.add(scssEstimate.sARCoef);
		initCoef.add(scssEstimate.sMACoef);
		initCoef.add(new double[] {scssEstimate.variance});

		//step 2: set residual[i]=0, if i<initPoint; set data[i]=0, if i<initPoint
		double[] cResidual = new double[seasonMatrix[0].length];

		SMLEGradientTarget smleGT = new SMLEGradientTarget();
		smleGT.fit(seasonMatrix, cResidual, initCoef, season);
		AbstractGradientTarget problem = BFGS.solve(smleGT,
			1000, 0.01, 0.000001, new int[] {1, 2, 3}, -1);

		this.sResidual = problem.getResidual();
		this.logLikelihood = -problem.getMinValue();

		DenseMatrix result = problem.getFinalCoef();
		this.warn = problem.getWarn();
		this.sARCoef = new double[sP];
		this.sMACoef = new double[sQ];
		for (int i = 0; i < sP; i++) {
			this.sARCoef[i] = result.get(i, 0);
		}
		for (int i = 0; i < sQ; i++) {
			this.sMACoef[i] = result.get(i + sP, 0);
		}
		this.variance = result.get(sP + sQ, 0);

		boolean gradientSuccess = Boolean.TRUE;

		if (problem.getIter() < 2) {
			gradientSuccess = Boolean.FALSE;
		}

		if (gradientSuccess) {
			DenseMatrix information = problem.getH();

			this.sArStdError = new double[sP];
			this.sMaStdError = new double[sQ];
			for (int i = 0; i < sP; i++) {
				this.sArStdError[i] = Math.sqrt(information.get(i, i));
				if (Double.isNaN(this.sArStdError[i])) {
					this.sArStdError[i] = -99;
				}
			}
			for (int i = 0; i < sQ; i++) {
				this.sMaStdError[i] = Math.sqrt(information.get(i + sP, i + sP));
				if (Double.isNaN(this.sMaStdError[i])) {
					this.sMaStdError[i] = -99;
				}
			}
			this.varianceStdError = Math.sqrt(information.get(sP + sQ, sP + sQ));
			if (Double.isNaN(this.varianceStdError)) {
				this.varianceStdError = -99;
			} else {
				this.sArStdError = scssEstimate.sArStdError;
				this.sMaStdError = scssEstimate.sMaStdError;
				this.varianceStdError = 1;
			}

		}
	}

}
