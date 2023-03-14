package com.alibaba.alink.operator.common.timeseries.arimagarch;

import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.garch.GarchModel;
import com.alibaba.alink.params.timeseries.HasArimaGarchMethod.ArimaGarchMethod;

import java.util.ArrayList;

import static com.alibaba.alink.params.timeseries.HasArimaGarchMethod.ArimaGarchMethod.SEPARATE;

/**
 * warn: warning for arima-garch model.
 * 1 for NaN Hessian Matrix. 2 for non causal.
 * 3 for non invertible. 4 for no no warned model in auto selection.
 * 5 for no heteroscedasticity on fitted residual
 */
public class ArimaGarchModel {

	public ModelInfo mi;
	public double[] data;
	public double[][] dData;

	public ArimaGarchMethod method;

	public ArimaModel arima;
	public GarchModel garch;

	public ArimaGarchModel() {
	}

	public ArrayList <double[]> forecast(int h) {
		switch (method) {
			case CONSIST:
				return consistForecast(h);
			case SEPARATE:
				return sepForecast(h);
			default:
				throw new RuntimeException("method is not support");
		}
	}

	private ArrayList <double[]> consistForecast(int h) {
		double[] hHat = new GarchModel().forecast(h, mi.hHat, mi.alpha, mi.beta, mi.c);
		double[] var = new double[h];

		if (mi.arCoef.length == 0 && mi.maCoef.length == 0) {
			for (int i = 0; i < h; i++) {
				var[i] = hHat[i];
			}
		} else {
			double[] cCoef = ArimaModel.causalCoef(h, mi.arCoef, mi.maCoef);
			double sum = 0;
			for (int i = 0; i < var.length; i++) {
				sum = sum + cCoef[i] * cCoef[i] * hHat[i];
				var[i] = sum;
			}
		}
		double[] se = new double[var.length];
		for (int i = 0; i < h; i++) {
			se[i] = Math.sqrt(var[i]);
		}

		return ArimaModel.forecast(h, se, mi.estResidual,
			mi.arCoef, mi.maCoef, mi.intercept, mi.sigma2,
			dData, mi.order[1], false);
	}

	private ArrayList <double[]> sepForecast(int h) {
		ArrayList <double[]> result;

		if (SEPARATE == this.method && !mi.ifHetero) {
			result = this.arima.forecast(h);
		} else {
			double[] hHat = this.garch.forecast(h);
			double[] var = new double[h];

			if (arima.arma.estimate.arCoef.length == 0 && arima.arma.estimate.maCoef.length == 0) {
				for (int i = 0; i < h; i++) {
					var[i] = hHat[i];
				}
			} else {
				double[] cCoef = ArimaModel.causalCoef(h,
					arima.arma.estimate.arCoef,
					arima.arma.estimate.maCoef);
				double sum = 0;
				for (int i = 0; i < var.length; i++) {
					sum = sum + cCoef[i] * cCoef[i] * hHat[i];
					var[i] = sum;
				}
			}
			double[] se = new double[var.length];
			for (int i = 0; i < h; i++) {
				se[i] = Math.sqrt(var[i]);
			}

			result = this.arima.forecast(h, se);
		}

		return result;
	}

	public boolean isGoodFit() {
		if (mi.warn != null) {
			for (int i = 0; i < mi.warn.size(); i++) {
				if (mi.warn.get(i).equals("4")) {
					return false;
				}
			}
		}
		return true;
	}

}
