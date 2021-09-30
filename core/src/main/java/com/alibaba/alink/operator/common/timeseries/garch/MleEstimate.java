package com.alibaba.alink.operator.common.timeseries.garch;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.timeseries.TimeSeriesUtils;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

import java.util.ArrayList;

public class MleEstimate {
	public double[] alpha;
	public double[] beta;
	public double c;
	public double[] alphaStdError;
	public double[] betaStdError;
	public double cStdError;
	public double[] h;
	public int aOrder;
	public int bOrder;
	public double loglike;
	public double unconSigma2;
	public ArrayList <String> warn;
	public double[] residual;

	public MleEstimate() {}

	public void bfgsEstimateParams(double[] data, int aOrder, int bOrder) {

		double mean = TsMethod.mean(data);
		for (int i = 0; i < data.length; i++) {
			data[i] -= mean;
		}

		this.aOrder = aOrder;
		this.bOrder = bOrder;

		//initiation of parameters
		double[] initAlpha = new double[aOrder];
		double[] initBeta = new double[bOrder];
		for (int i = 0; i < aOrder; i++) {
			initAlpha[i] = 0.1;
		}
		for (int i = 0; i < bOrder; i++) {
			initBeta[i] = 0.1;
		}

		double var = 0;
		for (double aData : data) {
			var += aData * aData;
		}
		var = var / data.length;

		double sumAlpha = 0;
		double sumBeta = 0;
		for (double alpha : initAlpha) {
			sumAlpha += alpha * alpha;
		}
		for (double beta : initBeta) {
			sumBeta += beta * beta;
		}
		double initC = Math.sqrt(var * (1 - sumAlpha - sumBeta));

		double[] ch = new double[data.length];
		for (int i = 0; i < bOrder; i++) {
			ch[i] = var;
		}

		/////////-----------------------
		//int m = this.aOrder + this.bOrder + 1 + 1;
		int m = this.aOrder + this.bOrder + 1;
		DenseVector lowerBound = DenseVector.zeros(m);
		DenseVector upperBound = DenseVector.ones(m);
		lowerBound.set(m - 1, var * 1e-8);
		upperBound.set(m - 1, var * 10);
		//lowerBound.set(m - 2, var * 1e-8);
		//upperBound.set(m - 2, var * 10);
		//lowerBound.set(m - 2, Double.NEGATIVE_INFINITY);
		//upperBound.set(m - 2, Double.POSITIVE_INFINITY);

		GarchGradientTarget garchGradientTarget = new GarchGradientTarget();
		garchGradientTarget.fit(initAlpha, initBeta, initC, data, ch);

		DenseVector result = TimeSeriesUtils.calcGarchLBFGSB(lowerBound,
			upperBound,
			garchGradientTarget,
			data,
			500,
			1e-8);

		this.alpha = new double[aOrder];
		this.beta = new double[bOrder];

		for (int i = 0; i < aOrder; i++) {
			alpha[i] = result.get(i);
		}
		for (int i = 0; i < bOrder; i++) {
			beta[i] = result.get(i + aOrder) * result.get(i + aOrder);
		}

		this.c = result.get(aOrder + bOrder) * result.get(aOrder + bOrder);

		this.h = garchGradientTarget.residual;

		///////////-----------------------
		////find parameters
		//GarchGradientTarget garchProblem = new GarchGradientTarget();
		//garchProblem.fit(initAlpha, initBeta, initC, data, ch);
		//AbstractGradientTarget problem = BFGS.solve(
		//	garchProblem, 1000, 0.00001,
		//	0.000001, new int[] {2, 3}, -1);
		//
		//this.alpha = new double[aOrder];
		//this.beta = new double[bOrder];
		//DenseMatrix result = problem.getFinalCoef();
		//for (int i = 0; i < aOrder; i++) {
		//	alpha[i] = result.get(i, 0) * result.get(i, 0);
		//}
		//for (int i = 0; i < bOrder; i++) {
		//	beta[i] = result.get(i + aOrder, 0) * result.get(i + aOrder, 0);
		//}
		//this.c = result.get(aOrder + bOrder, 0) * result.get(aOrder + bOrder, 0);
		//this.h = problem.getResidual();
		//this.loglike = -problem.getMinValue();
		//this.warn = problem.getWarn();
		//
		//DenseMatrix obsInformation = problem.getH();
		//this.alphaStdError = new double[aOrder];
		//this.betaStdError = new double[bOrder];
		//for (int i = 0; i < aOrder; i++) {
		//	this.alphaStdError[i] = Math.sqrt(obsInformation.get(i, i));
		//	if (Double.isNaN(this.alphaStdError[i])) {
		//		this.alphaStdError[i] = -99;
		//	}
		//
		//}
		//for (int i = 0; i < bOrder; i++) {
		//	this.betaStdError[i] = Math.sqrt(obsInformation.get(i + aOrder, i + aOrder));
		//	if (Double.isNaN(this.betaStdError[i])) {
		//		this.betaStdError[i] = -99;
		//	}
		//}
		//this.cStdError = Math.sqrt(obsInformation.get(aOrder + bOrder, aOrder + bOrder));
		//if (Double.isNaN(this.cStdError)) {
		//	this.cStdError = -99;
		//}
		//
		//sumAlpha = 0;
		//sumBeta = 0;
		//for (int i = 0; i < this.aOrder; i++) {
		//	sumAlpha += this.alpha[i];
		//}
		//for (int i = 0; i < this.bOrder; i++) {
		//	sumBeta += this.beta[i];
		//}
		//
		//this.unconSigma2 = this.c / (1 - sumAlpha - sumBeta);
		//this.residual = new double[this.h.length];
		//for (int i = 0; i < h.length; i++) {
		//	this.residual[i] = data[i] / h[i];
		//}

	}

}
