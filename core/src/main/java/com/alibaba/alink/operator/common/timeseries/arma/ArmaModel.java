package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
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
 * warn: warning in conducting gradient descent. 1 for NaN Hessian Matrix. 2 for non causal. 3 for non invertible
 */
public class ArmaModel {
	public int p;
	public int q;
	public EstMethod estMethod;
	public int ifIntercept;

	public double[] data;

	public ArmaEstimate estimate;

	public ArmaModel(int p, int q, EstMethod estMethod, int ifIntercept) {
		this.ifIntercept = ifIntercept;
		this.estMethod = estMethod;
		this.p = p;
		this.q = q;

		if (p < 0 || q < 0) {
			throw new AkIllegalOperatorParameterException("Order p and q need >= 0");
		}
	}

	/**
	 * estMethod: methods to estimate parameters. "Mom"(MOM), "Hr"(CSS-HannanRissanen), "Css"(CSS-zero
	 * initialization),"Css-mle"(conditional MLE), "mle"(exact)
	 * for MOM, Levinson algorithm is used to avoid unsolvable matrix.
	 * ifIntercept: 1(have intercept) and 0(No intercept)
	 */
	public void fit(double[] data) {
		if (data == null) {
			throw new AkIllegalDataException("data must be not null.");
		}
		this.data = data.clone();

		if (data.length - p < p + q + ifIntercept) {
			throw new AkIllegalDataException("Do not have enough data. data size need > 2p + q.");
		}

		if (ifIntercept != 1 && ifIntercept != 0) {
			throw new AkIllegalOperatorParameterException(
				"ifIntercept must be int 1(have intercept) or 0(no intercept)");
		}

		switch (estMethod) {
			case Mom:
				estimate = new MOMEstimate();
				break;
			case Hr:
				estimate = new HrEstimate();
				break;
			case CssMle:
				estimate = new CSSMLEEstimate();
				break;
			case Css:
				estimate = new CSSEstimate();
				break;
			default:
				throw new AkUnsupportedOperationException(String.format("Method [%s] not support. ", estimate));
		}

		estimate.compute(this.data, this.p, this.q, this.ifIntercept);
		estimate.checkCoef();
	}

	public ArrayList <double[]> forecast(int predictNum) {
		return forecast(predictNum,
			this.data,
			this.estimate.residual,
			this.estimate.arCoef,
			this.estimate.maCoef,
			this.estimate.intercept,
			this.estimate.variance);
	}

	/**
	 * diff: 1: add mean when doing forecast. 2: not add
	 * <p>
	 * <p>
	 * First row: prediction
	 * second row: standard error for each prediction
	 * third row: lower bound of 95% CI
	 * fourth row: upper bound of 95% CI
	 */
	public static ArrayList <double[]> forecast(int predictNum, double[] data,
												double[] estResidual, double[] arCoef,
												double[] maCoef,
												double intercept, double variance) {
		if (predictNum <= 0) {
			throw new AkIllegalOperatorParameterException("Number of predicted need > 0");
		}

		ArrayList <double[]> result = new ArrayList <>();

		//step 1: predict values from end of data up to predictNum
		double[] resultVariance;
		double[] predictResult = valuePredict(data, estResidual, arCoef, maCoef, intercept, predictNum);

		//step 2: calculate variance for each value
		if (arCoef.length == 0 && maCoef.length == 0) {
			resultVariance = new double[predictNum];
			Arrays.fill(resultVariance, variance);
		} else {
			resultVariance = varCompute(
				predictNum,
				variance,
				ArimaModel.causalCoef(predictNum, arCoef, maCoef));
		}

		double[] lower = new double[predictNum];
		double[] upper = new double[predictNum];
		for (int i = 0; i < predictNum; i++) {
			resultVariance[i] = resultVariance[i] < 0 ? 0 : Math.sqrt(resultVariance[i]);
			lower[i] = predictResult[i] - 1.96 * resultVariance[i];
			upper[i] = predictResult[i] + 1.96 * resultVariance[i];
		}

		result.add(predictResult);
		result.add(resultVariance);
		result.add(lower);
		result.add(upper);

		return result;
	}

	private static double[] valuePredict(double[] data,
										 double[] residual,
										 double[] arCoef,
										 double[] maCoef,
										 double intercept,
										 int predictNum) {
		double[] newData = new double[data.length + predictNum];
		double[] newResidual = new double[residual.length + predictNum];
		double[] result = new double[predictNum];
		if (arCoef.length == 0 && maCoef.length == 0) {
			Arrays.fill(result, intercept);
		} else {
			System.arraycopy(data, 0, newData, 0, data.length);
			System.arraycopy(residual, 0, newResidual, 0, residual.length);

			for (int i = 0; i < predictNum; i++) {
				double arSum = 0;
				for (int j = 0; j < arCoef.length; j++) {
					arSum = arSum + arCoef[j] * newData[data.length + i - j - 1];
				}

				double maSum = 0;
				for (int j = 0; j < maCoef.length; j++) {
					maSum = maSum + maCoef[j] * newResidual[residual.length + i - j - 1];
				}

				newData[data.length + i] = arSum + maSum + intercept;
			}

			System.arraycopy(newData, data.length, result, 0, predictNum);
		}
		return result;
	}

	static double[] varCompute(int h, double sigma2, double[] cCoef) {
		double[] var = new double[h];
		double sum = 0;
		for (int i = 0; i < var.length; i++) {
			sum = sum + cCoef[i] * cCoef[i];
			var[i] = sigma2 * sum;
		}
		return var;
	}

}
