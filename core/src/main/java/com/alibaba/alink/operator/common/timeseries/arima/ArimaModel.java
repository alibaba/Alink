package com.alibaba.alink.operator.common.timeseries.arima;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.common.timeseries.arma.ArmaModel;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * p: AR order
 * q: MA order
 * data: origin data
 * mean: sample data mean
 * sARCoef: coefficients of AR part
 * maCoef: coefficients of MA part
 * intercept: intercept in ARMA model
 * Css: condition sum of square
 * variance: estimated variance of residual
 * logLikelihood: log likelihood
 * estResidual: estimated residual sequence
 * estMethod: method of parameter estimation
 * ifItercept: if the model includes intercept
 * warn: warning in conducting gradient descent. 1 for NaN Hessian Matrix. 2 for non causal. 3 for non
 * invertible. 4
 * for no no warned model in auto selection.
 */
public class ArimaModel {

	public int p;
	public int d;
	public int q;
	public EstMethod estMethod;
	public int ifIntercept;
	public double ic;
	public boolean isMean;

	double[][] diffData;

	public ArmaModel arma;

	public ArimaModel() {
		this.isMean = true;
	}

	public ArimaModel(int p, int d, int q, EstMethod estMethod, int ifIntercept) {
		this.p = p;
		this.d = d;
		this.q = q;

		if (this.p < 0 || this.q < 0 || this.d < 0) {
			throw new AkIllegalOperatorParameterException("Order p, d and q need >= 0.");
		}

		this.ifIntercept = ifIntercept;
		this.estMethod = estMethod;

		this.arma = new ArmaModel(this.p, this.q, this.estMethod, this.ifIntercept);
	}

	/*
	 * For the result:
	 *  First row: prediction
	 *  second row: standard error for each prediction
	 *  third row: lower bound of 95% CI
	 *  fourth row: upper bound of 95% CI
	 */
	public ArrayList <double[]> forecast(int predictNum) {
		ArrayList <double[]> result = ArmaModel.forecast(predictNum,
			diffData[0], this.arma.estimate.residual,
			this.arma.estimate.arCoef,
			this.arma.estimate.maCoef,
			this.arma.estimate.intercept,
			this.arma.estimate.variance);
		return forecast(predictNum, result.get(1));
	}

	public ArrayList <double[]> forecast(int preditNum, double[] stdErrors) {
		return forecast(preditNum, stdErrors,
			this.arma.estimate.residual,
			this.arma.estimate.arCoef, this.arma.estimate.maCoef,
			this.arma.estimate.intercept, this.arma.estimate.variance,
			this.diffData, this.d, this.isMean);
	}

	/**
	 * used for heteroskedasticity variance
	 */
	public static ArrayList <double[]> forecast(int predictNum, double[] stdErrors,
												double[] estResidual,
												double[] arCoef, double[] maCoef,
												double intercept, double variance,
												double[][] diffData, int d,
												boolean isMean) {
		double[] prediction = new double[predictNum];
		if (isMean) {
			Arrays.fill(prediction, TsMethod.mean(diffData[0]));
		} else {
			ArrayList <double[]> result = ArmaModel.forecast(predictNum, diffData[0],
				estResidual, arCoef, maCoef, intercept, variance);

			double[][] predictions = new double[d + 1][predictNum];
			predictions[d] = result.get(0);
			for (int i = d - 1; i >= 0; i--) {
				predictions[i] = TsMethod.diffReduction(diffData[i], predictions[i + 1]);
			}
			prediction = predictions[0];
		}

		double[] lower = new double[predictNum];
		double[] upper = new double[predictNum];
		for (int i = 0; i < predictNum; i++) {
			lower[i] = prediction[i] - 1.96 * stdErrors[i];
			upper[i] = prediction[i] + 1.96 * stdErrors[i];
		}

		ArrayList <double[]> out = new ArrayList <>();
		out.add(prediction);
		out.add(stdErrors);
		out.add(lower);
		out.add(upper);

		return out;
	}

	public static double[] causalCoef(int h, double[] arCoef, double[] maCoef) {
		double[] cCoef = new double[h];
		cCoef[0] = 1;
		for (int i = 1; i < cCoef.length; i++) {
			for (int j = 1; j <= i && j <= arCoef.length; j++) {
				cCoef[i] = cCoef[i] + arCoef[j - 1] * cCoef[i - j];
			}
			if (i <= maCoef.length) {
				cCoef[i] = cCoef[i] + maCoef[i - 1];
			}
		}
		return cCoef;
	}

	public boolean isGoodFit() {
		if (arma.estimate.warn != null) {
			for (int i = 0; i < arma.estimate.warn.size(); i++) {
				if (arma.estimate.warn.get(i).equals("4")) {
					return false;
				}
			}
		}
		return true;
	}
}