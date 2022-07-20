package com.alibaba.alink.operator.common.timeseries.garch;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;

import java.util.ArrayList;

public class Garch {

	public static GarchModel fit(double[] data, int aOrder, int bOrder, boolean minusMean) {
		GarchModel model = new GarchModel();
		model.data = data.clone();
		model.aOrder = aOrder;
		model.bOrder = bOrder;

		if (aOrder < 0 || bOrder < 0) {
			throw new AkIllegalOperatorParameterException("Order for conditional variance and data must equal to or Greater than 0");
		}
		if (aOrder == 0 && bOrder == 0) {
			throw new AkIllegalOperatorParameterException("Order for conditional variance and data can not be 0 simultaneously");
		}

		if (data.length - bOrder < aOrder + bOrder + 1) {
			throw new AkIllegalOperatorParameterException("Do not have enough data");
		}

		MleEstimate mleEstimate = new MleEstimate();
		mleEstimate.bfgsEstimateParams(model.data, aOrder, bOrder);
		model.data = data;
		model.alpha = mleEstimate.alpha;
		model.beta = mleEstimate.beta;
		model.c = mleEstimate.c;
		model.hHat = mleEstimate.h;
		model.loglike = mleEstimate.loglike;
		model.seAlpha = mleEstimate.alphaStdError;
		model.seBeta = mleEstimate.betaStdError;
		model.seC = mleEstimate.cStdError;
		model.unconSigma2 = mleEstimate.unconSigma2;
		model.residual = mleEstimate.residual;
		model.warn = mleEstimate.warn;

		double sumAlpha = 0;
		double sumBeta = 0;
		for (int q = 0; q < aOrder; q++) {
			sumAlpha += model.alpha[q];
		}
		for (int q = 0; q < bOrder; q++) {
			sumBeta += model.beta[q];
		}

		if (model.warn == null) {
			model.warn = new ArrayList <>();
		}

		if (sumAlpha + sumBeta >= 1) {
			model.warn.add("5");
		}

		return model;
	}

	public static GarchModel autoFit(double[] data,
									 int upperBound,
									 boolean minusMean,
									 IcType ic,
									 boolean ifGarch11) {

		if (upperBound <= 0) {
			throw new AkIllegalOperatorParameterException("upperBound must be Greater than 0");
		}

		if (!ifGarch11) {
			double bestIC = Double.MAX_VALUE;
			GarchModel bestGARCH = null;

			for (int i = 0; i <= upperBound; i++) {
				for (int j = 0; j <= upperBound; j++) {
					if (i == 0 && j == 0) {
						continue;
					}

					GarchModel g = fit(data, i, j, minusMean);

					double gIC = TsMethod.icCompute(g.loglike, i + j + 1, data.length - Math.max(i, j), ic,
						Boolean.TRUE);

					double sumAlpha = 0;
					double sumBeta = 0;
					for (int q = 0; q < g.aOrder; q++) {
						sumAlpha += g.alpha[q];
					}
					for (int q = 0; q < g.bOrder; q++) {
						sumBeta += g.beta[q];
					}
					if (sumAlpha + sumBeta >= 1) {
						if (g.warn == null) {
							g.warn = new ArrayList <>();
						}
						g.warn.add("5");
					}

					if (g.warn != null) {
						continue;
					}

					if (gIC < bestIC) {
						bestIC = gIC;
						bestGARCH = g;
					}
				}
			}

			if (bestIC == Double.MAX_VALUE) {
				bestGARCH.warn.add("4");
			}

			bestGARCH.ic = bestIC;

			return bestGARCH;

		} else {
			GarchModel g = fit(data, 1, 1, minusMean);

			g.ic = TsMethod.icCompute(g.loglike, 3, data.length - 1, ic, true);

			if (g.warn != null) {
				g.warn.add("4");
			}
			return g;
		}
	}

}
