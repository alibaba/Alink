package com.alibaba.alink.operator.common.timeseries.arimagarch;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.BFGS;
import com.alibaba.alink.operator.common.timeseries.arma.ArmaModel;
import com.alibaba.alink.operator.common.timeseries.teststatistics.StationaryTest;
import com.alibaba.alink.params.timeseries.HasEstmateMethod;

import java.util.ArrayList;

public class ConsistEstimate {

	public ModelInfo mi;

	public ConsistEstimate() {
	}

	public void bfgsEstimate(double[] data, int[] order1, int[] order2, int ifIntercept) {

		double[] initalpha = new double[order2[0]];
		double[] initbeta = new double[order2[1]];
		for (int i = 0; i < order2[0]; i++) {
			initalpha[i] = 0.1;
		}
		for (int i = 0; i < order2[1]; i++) {
			initbeta[i] = 0.1;
		}

		ArmaModel arma = new ArmaModel(order1[0], order1[1], HasEstmateMethod.EstMethod.Css, ifIntercept);
		arma.fit(data);
		double[] initAR = arma.estimate.arCoef.clone();
		double[] initMA = arma.estimate.maCoef.clone();
		double initIntercept = arma.estimate.intercept;

		double var = 0;
		int n =  arma.estimate.residual.length;
		for (int i = order1[0]; i < n; i++) {
			var += data[i] * data[i];
		}
		var = var / (n - order1[0]);

		double sumAlpha = 0;
		double sumBeta = 0;
		for (int i = 0; i < initalpha.length; i++) {
			sumAlpha += initalpha[i] * initbeta[i];
		}
		for (int i = 0; i < initbeta.length; i++) {
			sumBeta += initbeta[i] * initbeta[i];
		}
		double initc = Math.sqrt(arma.estimate.variance * (1 - sumAlpha - sumBeta));

		double[] ch = new double[n - order1[0]];
		for (int i = 0; i < order2[1]; i++) {
			ch[i] = var;
		}

		double[] cResidual = new double[data.length];

		ArrayList <double[]> initCoef = new ArrayList <>();
		initCoef.add(initAR);
		initCoef.add(initMA);
		initCoef.add(new double[] {initIntercept});
		initCoef.add(initalpha);
		initCoef.add(initbeta);
		initCoef.add(new double[] {initc});

		ArimaGarchGradientTarget agp = new ArimaGarchGradientTarget();
		agp.fit(initCoef, data, cResidual, ch, ifIntercept);
		AbstractGradientTarget problem = new BFGS().solve(agp,
			500, 0.000001, 0.000001, new int[] {1, 2, 3}, -1);
		this.mi = new ModelInfo();
		this.mi.loglike = -problem.getMinValue();
		this.mi.warn = problem.getWarn();

		ArrayList <double[]> params = agp.trans(problem.getFinalCoef());
		this.mi.arCoef = params.get(0);
		this.mi.maCoef = params.get(1);
		this.mi.intercept = params.get(2)[0];
		this.mi.alpha = params.get(3);
		for (int i = 0; i < mi.alpha.length; i++) {
			mi.alpha[i] = mi.alpha[i] * mi.alpha[i];
		}
		this.mi.beta = params.get(4);
		for (int i = 0; i < mi.alpha.length; i++) {
			mi.beta[i] = mi.beta[i] * mi.beta[i];
		}
		this.mi.c = params.get(5)[0] * params.get(5)[0];

		boolean gradientSuccess = Boolean.TRUE;

		if (problem.getIter() < 2) {
			gradientSuccess = Boolean.FALSE;
		}

		if (gradientSuccess) {
			DenseMatrix obsInformation = problem.getH();
			this.mi.seARCoef = new double[order1[0]];
			this.mi.seMACoef = new double[order1[1]];
			this.mi.seAlpha = new double[order2[0]];
			this.mi.seBeta = new double[order2[1]];

			for (int i = 0; i < order1[0]; i++) {
				this.mi.seARCoef[i] = Math.sqrt(obsInformation.get(i, i));
				if (Double.isNaN(this.mi.seARCoef[i])) {
					this.mi.seARCoef[i] = -99;
				}
			}
			for (int i = 0; i < order1[1]; i++) {
				this.mi.seMACoef[i] = Math.sqrt(obsInformation.get(i + order1[0], i + order1[0]));
				if (Double.isNaN(this.mi.seMACoef[i])) {
					this.mi.seMACoef[i] = -99;
				}
			}
			if (ifIntercept == 1) {
				this.mi.seIntercept = Math.sqrt(obsInformation.get(order1[1] + order1[0], order1[1] + order1[0]));
				if (Double.isNaN(this.mi.seIntercept)) {
					this.mi.seIntercept = -99;
				}
			} else {
				this.mi.seIntercept = 0;
			}

			for (int i = 0; i < order2[0]; i++) {
				this.mi.seAlpha[i] = Math.sqrt(
					obsInformation.get(i + order1[0] + order1[1] + 1, i + order1[0] + order1[1] + 1));
				if (Double.isNaN(this.mi.seAlpha[i])) {
					this.mi.seAlpha[i] = -99;
				}
			}
			for (int i = 0; i < order2[1]; i++) {
				this.mi.seBeta[i] = Math.sqrt(obsInformation
					.get(i + order1[0] + order1[1] + 1 + order2[0], i + order1[0] + order1[1] + 1 + order2[0]));
				if (Double.isNaN(this.mi.seBeta[i])) {
					this.mi.seBeta[i] = -99;
				}
			}
			this.mi.seC = Math.sqrt(obsInformation.get(order1[0] + order1[1] + ifIntercept + order2[0] + order2[1],
				order1[0] + order1[1] + ifIntercept + order2[0] + order2[1]));
			if (Double.isNaN(this.mi.seC)) {
				this.mi.seC = -99;
			}
		} else {
			this.mi.seARCoef = arma.estimate.arCoefStdError;
			this.mi.seMACoef = arma.estimate.maCoefStdError;
			this.mi.seIntercept = arma.estimate.interceptStdError;

			this.mi.seAlpha = new double[order2[0]];
			for (int i = 0; i < order2[0]; i++) {
				this.mi.seAlpha[i] = 1;
			}

			this.mi.seBeta = new double[order2[1]];
			for (int i = 0; i < order2[1]; i++) {
				this.mi.seBeta[i] = 1;
			}

			this.mi.seC = 1;
		}

		ArrayList <double[]> residuals = agp.findSeries(agp.data, params);
		this.mi.estResidual = residuals.get(0);
		this.mi.hHat = residuals.get(1);

		int h = Math.min(this.mi.estResidual.length / 5, 10);
		double[][] test = new StationaryTest().ljungBox(this.mi.estResidual, 0.95, h);
		if (test[0][0] <= test[1][0]) {
			mi.ifHetero = Boolean.FALSE;
		} else {
			mi.ifHetero = Boolean.TRUE;
		}
		if (mi.ifHetero == Boolean.FALSE) {
			if (mi.warn == null) {
				mi.warn = new ArrayList <>();
			}
			mi.warn.add("5");
		}

		sumAlpha = 0;
		sumBeta = 0;
		for (int i = 0; i < order2[0]; i++) {
			sumAlpha += mi.alpha[i];
		}
		for (int i = 0; i < order2[1]; i++) {
			sumBeta += mi.beta[i];
		}

		mi.unconSigma2 = mi.c / (1 - sumAlpha - sumBeta);

	}

}
