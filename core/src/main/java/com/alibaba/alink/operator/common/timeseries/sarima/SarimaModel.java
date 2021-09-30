package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;

import java.util.ArrayList;

public class SarimaModel {
	public int p;
	public int d;
	public int q;
	public int sP;
	public int sD;
	public int sQ;
	public int seasonalPeriod;
	public int ifIntercept;
	public EstMethod estMethod;

	public double[][] seasonDData;
	public double[][] seasonMatrix;

	public ArrayList <String> warn;
	public double ic;
	public int sampleSize;

	public double[] sARCoef;
	public double[] sMACoef;
	public double[] sArStdError;
	public double[] sMaStdError;
	public double[] sResidual;

	public ArimaModel arima;

	public SarimaModel(int p, int d, int q,
					   int sP, int sD, int sQ,
					   EstMethod estMethod,
					   int ifIntercept,
					   int seasonalPeriod) {
		this.p = p;
		this.d = d;
		this.q = q;
		this.sP = sP;
		this.sD = sD;
		this.sQ = sQ;
		this.estMethod = estMethod;
		this.ifIntercept = ifIntercept;
		this.seasonalPeriod = seasonalPeriod;
	}

	public ArrayList <double[]> forecast(int predictNum) {
		ArrayList <double[]> arimaResult = arima.forecast(predictNum);
		if (seasonalPeriod == 1) {
			return arimaResult;
		} else {
			double[][] prediction = new double[sD + 1][predictNum];

			prediction[sD] = seasonalForecast(arimaResult, seasonDData[sD],
				sResidual, sARCoef,
				sMACoef, seasonalPeriod);

			for (int i = sD - 1; i >= 0; i--) {
				prediction[i] = seasonDiffReturn(seasonDData[i], prediction[i + 1], seasonalPeriod);
			}

			ArrayList <double[]> out = new ArrayList <>();
			double[] lower = new double[predictNum];
			double[] upper = new double[predictNum];
			double[] sResidualStd = arimaResult.get(1);
			for (int i = 0; i < predictNum; i++) {
				lower[i] = prediction[0][i] - 1.96 * sResidualStd[i];
				upper[i] = prediction[0][i] + 1.96 * sResidualStd[i];
			}

			out.add(prediction[0]);
			out.add(arimaResult.get(1));
			out.add(lower);
			out.add(upper);

			return out;
		}
	}

	/**
	 * change seasonally differenced prediction back to the original level
	 */
	private static double[] seasonDiffReturn(double[] data, double[] outcome, int season) {
		double[] newData = new double[data.length + outcome.length];
		System.arraycopy(data, 0, newData, 0, data.length);
		System.arraycopy(outcome, 0, newData, data.length, outcome.length);

		double[] outcomeR = new double[outcome.length];
		for (int i = 0; i < outcome.length; i++) {
			outcomeR[i] = outcome[i] + newData[i + data.length - season];
			newData[i + data.length] = outcomeR[i];
		}
		return outcomeR;
	}

	private static double[] seasonalForecast(ArrayList <double[]> arimaResult,
											 double[] data, double[] sResidual,
											 double[] sArCoef, double[] sMaCoef,
											 int seasonPeriod) {
		double[] nonSeasonPoint = arimaResult.get(0);

		int step = nonSeasonPoint.length;
		double[] foreData = new double[data.length + step];
		double[] foreResidual = new double[sResidual.length + step];

		double[] finalData = new double[step];

		System.arraycopy(data, 0, foreData, 0, data.length);
		System.arraycopy(sResidual, 0, foreResidual, 0, sResidual.length);

		for (int i = 0; i < step; i++) {
			foreResidual[i + sResidual.length] = nonSeasonPoint[i];
			double oneStepX = foreResidual[i + sResidual.length];

			if (sArCoef != null) {
				for (int j = 0; j < sArCoef.length; j++) {
					oneStepX += sArCoef[j] * foreResidual[i + sResidual.length - (j + 1) * seasonPeriod];
				}
			}
			if (sMaCoef != null) {
				for (int j = 0; j < sMaCoef.length; j++) {
					oneStepX += sMaCoef[j] * foreData[i + data.length - (j + 1) * seasonPeriod];
				}
			}

			foreData[i + data.length] = oneStepX;
			finalData[i] = oneStepX;
		}

		return finalData;
	}

	public boolean isGoodFit() {
		boolean ifGoodFit = true;
		if (warn != null) {
			for (String aWarn : warn) {
				if (aWarn.equals("4")) {
					ifGoodFit = false;
				}
			}
		}
		return ifGoodFit;
	}

}
