package com.alibaba.alink.operator.common.timeseries.garch;

import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.probabilistic.XRandom;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

import java.util.ArrayList;

/**
 * data is defined as a zero mean series
 * <p>
 * alpha: coefficients of h part
 * beta: coefficients of data part
 * c: constant
 * h: estimated h series, which is the variance of data conditioned on history data
 * logLikelihood: log likelihood of model
 * unconSigma2: unconditional variance of data
 * warn: warning in garch model.
 * 1 for NaN Hessian Matrix. 2 for non causal.
 * 3 for non invertible. 4 for no no warned model in auto selection.
 * 5 for not stationary because sum of coefficients Greater than one
 */
public class GarchModel {

	public double[] alpha;
	public double[] beta;
	public double c;
	public double[] seAlpha;
	public double[] seBeta;
	public double seC;
	public double[] hHat;
	public double[] data;
	public int aOrder;
	public int bOrder;
	public double loglike;
	public double unconSigma2;
	public ArrayList <String> warn;
	public double[] residual;
	public double ic;

	public GarchModel() {
	}

	public double[] forecast(int step) {
		return forecast(step, this.hHat, this.alpha, this.beta, this.c);
	}

	public double[] forecast(int step, double[] h, double[] alpha, double[] beta, double c) {
		int aOrder = alpha.length;
		int bOrder = beta.length;
		double[] hHat = new double[h.length + step];
		for (int i = 0; i < h.length; i++) {
			hHat[i] = h[i];
		}
		if (aOrder == 1 && bOrder == 1) {
			for (int i = 0; i < step; i++) {
				hHat[i + h.length] = c + (alpha[0] + beta[0]) * hHat[i + h.length - 1];
			}
		} else {
			for (int i = 0; i < step; i++) {
				double sumAlpha = 0;
				double sumBeta = 0;
				for (int j = 0; j < aOrder; j++) {
					sumAlpha += alpha[j] * hHat[i + h.length - 1 - j];
				}
				for (int j = 0; j < bOrder; j++) {
					sumBeta += beta[j] * hHat[i + h.length - 1 - j];
				}

				hHat[i + h.length] = c + sumAlpha + sumBeta;
			}
		}

		double[] var = new double[step];
		for (int i = 0; i < step; i++) {
			var[i] = hHat[i + h.length];
		}

		double mean = TsMethod.mean(data);
		double[] forecast = new double[step];
		XRandom random = new XRandom();
		random.setSeed(1L);
		for (int i = 0; i < step; i++) {
			forecast[i] =  random.normalDistArray(1, mean,  var[i])[0];
		}
		return forecast;
	}

	public boolean isGoodFit() {
		if (warn != null) {
			for (String aWarn : warn) {
				if (aWarn.equals("4")) {
					return false;
				}
			}
		}
		return true;
	}
}
