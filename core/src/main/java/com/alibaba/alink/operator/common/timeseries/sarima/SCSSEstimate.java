package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

import java.util.ArrayList;

public class SCSSEstimate extends SarimaEstimate {

	public double css;

	public void compute(double[][] seasonMatrix, int sP, int sQ, int season) {
		int dataLength = TsMethod.seasonMatrix2Array(seasonMatrix).length;

		//step 1: set residual[i]=0, if i<initPoint; set data[i]=0, if i<initPoint
		double[] cResidual = new double[seasonMatrix[0].length];

		//step 2: find initial parameters for fitting CSS
		double[] newARCoef = new double[sP];
		double[] newMACoef = new double[sQ];
		double[] newIntercept = new double[1];
		ArrayList <double[]> initCoef = new ArrayList <>();
		initCoef.add(newARCoef);
		initCoef.add(newMACoef);
		initCoef.add(newIntercept);

		//step 3: gradient descent
		SCSSGradientTarget sarmaGT = new SCSSGradientTarget();
		sarmaGT.fit(seasonMatrix, cResidual, initCoef, season);
		AbstractGradientTarget problem = BFGS.solve(sarmaGT,
			1000, 0.001, 0.000001, new int[] {1, 2, 3}, -1);

		this.sResidual = problem.getResidual();
		this.variance = problem.getMinValue() / (dataLength - sP - sQ);

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

		this.css = problem.getMinValue();
		this.logLikelihood = -((double) (dataLength - sP) / 2) * Math.log(2 * Math.PI)
			- ((double) (dataLength - sP) / 2) * Math.log(variance)
			- css / (2 * variance);

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

	}
}
