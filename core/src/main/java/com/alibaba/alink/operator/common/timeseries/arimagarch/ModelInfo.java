package com.alibaba.alink.operator.common.timeseries.arimagarch;

import java.util.ArrayList;

public class ModelInfo {
	public double[] arCoef;
	public double[] maCoef;
	public double intercept;
	public double sigma2;
	public double[] seARCoef;
	public double[] seMACoef;
	public double seIntercept;
	public double seSigma2;
	public double[] estResidual;

	public double[] alpha;
	public double[] beta;
	public double c;
	public double[] seAlpha;
	public double[] seBeta;
	public double seC;
	public double unconSigma2;
	public double[] hHat;
	public double[] e;

	public int[] order;
	public boolean ifHetero;
	public double loglike;
	public ArrayList <String> warn;
	public double ic;

	public ModelInfo() {}

	public ModelInfo copy() {
		ModelInfo mi = new ModelInfo();
		mi.ifHetero = this.ifHetero;
		mi.arCoef = this.arCoef;
		mi.maCoef = this.maCoef;
		mi.intercept = this.intercept;
		mi.seARCoef = this.seARCoef;
		mi.seMACoef = this.seMACoef;
		mi.seIntercept = this.seIntercept;
		mi.sigma2 = this.sigma2;
		mi.seSigma2 = this.seSigma2;
		mi.estResidual = this.estResidual;

		mi.alpha = this.alpha;
		mi.beta = this.beta;
		mi.c = this.c;
		mi.seAlpha = this.seAlpha;
		mi.seBeta = this.seBeta;
		mi.seC = this.seC;
		mi.unconSigma2 = this.unconSigma2;
		mi.hHat = this.hHat;
		mi.e = this.e;

		mi.ifHetero = this.ifHetero;
		mi.loglike = this.loglike;
		mi.warn = this.warn;
		mi.ic = this.ic;

		mi.order = this.order;
		return mi;
	}

}
