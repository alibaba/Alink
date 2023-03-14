package com.alibaba.alink.operator.common.timeseries.arimagarch;

import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.arimagarch.ModelInfo;
import com.alibaba.alink.operator.common.timeseries.garch.Garch;
import com.alibaba.alink.operator.common.timeseries.garch.GarchModel;
import com.alibaba.alink.operator.common.timeseries.teststatistics.StationaryTest;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;
import com.alibaba.alink.params.timeseries.HasIcType;

public class SeparateEstimate {
	public ModelInfo mi;
	public ArimaModel arima;
	public GarchModel garch;

	public SeparateEstimate() {
		mi = new ModelInfo();
	}

	public void bfgsEstimate(double[] data, HasIcType.IcType ic, int maxARIMA, int maxGARCH, boolean ifGARCH11) {
		ArimaModel arima = Arima.autoFit(data, maxARIMA, HasEstmateMethod.EstMethod.CssMle, ic, -1);
		double[] residual2;
		int initPoint = Math.max(arima.p, arima.q);
		residual2 = new double[arima.arma.estimate.residual.length - initPoint];

		for (int i = 0; i < residual2.length; i++) {
			residual2[i] = arima.arma.estimate.residual[i + initPoint] * arima.arma.estimate.residual[i + initPoint];
		}

		//record arima info
		mi.arCoef = arima.arma.estimate.arCoef;
		mi.maCoef = arima.arma.estimate.maCoef;
		mi.intercept = arima.arma.estimate.intercept;
		mi.seARCoef = arima.arma.estimate.arCoefStdError;
		mi.seMACoef = arima.arma.estimate.maCoefStdError;
		mi.seIntercept = arima.arma.estimate.interceptStdError;
		mi.sigma2 = arima.arma.estimate.variance;
		mi.seSigma2 = arima.arma.estimate.varianceStdError;
		mi.estResidual = arima.arma.estimate.residual;
		mi.loglike = arima.arma.estimate.logLikelihood;
		mi.ic = 0;
		mi.warn = arima.arma.estimate.warn;
		this.arima = arima;
		//test estimated residual
		int h = Math.min(residual2.length / 5, 10);
		double[][] test = new StationaryTest().ljungBox(residual2, 0.95, h);
		if (test[0][0] <= test[1][0]) {
			mi.ifHetero = Boolean.FALSE;
			mi.order = new int[] {arima.p, arima.d, arima.q, 0, 0};
		} else {
			mi.ifHetero = Boolean.TRUE;
			this.garch = Garch.autoFit(arima.arma.estimate.residual, maxGARCH, false, ic, ifGARCH11);
			mi.order = new int[] {arima.p, arima.d, arima.q, this.garch.aOrder, this.garch.bOrder};
			mi.alpha = this.garch.alpha;
			mi.beta = this.garch.beta;
			mi.c = this.garch.c;
			mi.seAlpha = this.garch.seAlpha;
			mi.seBeta = this.garch.seAlpha;
			mi.seC = this.garch.seC;
			mi.hHat = this.garch.hHat;
			mi.unconSigma2 = this.garch.unconSigma2;
			mi.e = this.garch.residual;
			mi.loglike = this.garch.loglike;
			mi.ic = this.garch.ic;
			if (mi.warn == null) {
				mi.warn = this.garch.warn;
			} else {
				mi.warn.addAll(this.garch.warn);
			}

		}
	}

}
