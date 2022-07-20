package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.TsMethod;

import java.util.ArrayList;

public class SMLEGradientTarget extends AbstractGradientTarget {
	private double[][] seasonMatrix;
	private double[] cResidual;
	public int p;
	public int q;
	public int length;
	public double css;
	private double[][] iterResidualMatrix;

	public SMLEGradientTarget() {}

	/**
	 * for seasonal ARIMA, there is no intercept
	 */
	public void fit(double[][] seasonMatrix, double[] cResidual, ArrayList <double[]> initCoef, int season) {
		double[] arCoef = initCoef.get(0);
		double[] maCoef = initCoef.get(1);
		double sigma2 = initCoef.get(2)[0];
		this.p = arCoef.length;
		this.q = maCoef.length;
		this.seasonMatrix = seasonMatrix;
		this.cResidual = cResidual;

		double[] data = TsMethod.seasonMatrix2Array(seasonMatrix);
		this.length = data.length;
		double[][] x = new double[length][1];
		for (int i = 0; i < length; i++) {
			x[i][0] = data[i];
		}
		super.setX(new DenseMatrix(x));
		super.sampleSize = this.length - season * p;

		double[][] m = new double[arCoef.length + maCoef.length + 1][1];

		for (int i = 0; i < arCoef.length; i++) {
			m[i][0] = arCoef[i];
		}
		for (int i = 0; i < maCoef.length; i++) {
			m[i + arCoef.length][0] = maCoef[i];
		}
		m[p + q][0] = sigma2;
		if (m.length == 0) {
			super.initCoef = DenseMatrix.zeros(0, 0);
		} else {
			super.initCoef = new DenseMatrix(m);
		}
	}

	private double multiComputeMLE(double[][] seasonMatrix, double[] cResidual, double[] arCoef, double[] maCoef,
								   double sigma2) {
		SCSSGradientTarget cssGT = new SCSSGradientTarget();
		double rss = cssGT.multiComputeRSS(seasonMatrix, cResidual, arCoef, maCoef);
		this.iterResidualMatrix = cssGT.iterResidualMatrix;

		double loglike = ((double) super.sampleSize / 2.0) * Math.log(2 * Math.PI)
			+ ((double) super.sampleSize / 2.0) * (sigma2 <= 0 ? 0 : Math.log(sigma2));
		loglike += rss / (2.0 * sigma2);

		return loglike;
	}

	/**
	 * param: 1 for AR, 2 for MA, 3 for variance
	 */
	private double multiPComputeMLE(int param, int coefLag, double[][] seasonMatrix, double[][] iterResidualMatrix,
									double[] cResidual, double[] arCoef, double[] maCoef, double sigma2) {
		double gradient;
		SCSSGradientTarget cssGT = new SCSSGradientTarget();
		if (param == 3) {
			double rss = cssGT.multiComputeRSS(seasonMatrix, cResidual, arCoef, maCoef);
			gradient = ((double) super.sampleSize / (2 * sigma2)) - (rss / (2 * sigma2 * sigma2));
		} else {
			gradient = cssGT.multiPComputeRSS(param, coefLag, seasonMatrix, iterResidualMatrix, arCoef, maCoef) / (2
				* sigma2);
		}

		return gradient;
	}

	@Override
	public DenseMatrix gradient(DenseMatrix coef, int iter) {
		if (coef.numRows() != p + q + 1) {
			throw new AkIllegalDataException("Coef is not comparable with the model.");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}
		double sigma2 = coef.get(this.p + this.q, 0);

		//compute estimated residual
		this.multiComputeMLE(this.seasonMatrix, this.cResidual, arCoef, maCoef, sigma2);

		double[][] newGradient = new double[p + q + 1][1];

		//parameters gradient of AR
		for (int j = 0; j < this.p; j++) {
			//partial Inverse of ComputeRSS at sARCoef[j]
			newGradient[j][0] = this.multiPComputeMLE(1, j, this.seasonMatrix, this.iterResidualMatrix, this.cResidual,
				arCoef, maCoef, sigma2);
		}
		//parameters gradient of MA
		for (int j = 0; j < this.q; j++) {
			//partial Inverse of ComputeRSS at maCoef[j]
			newGradient[j + this.p][0] = this.multiPComputeMLE(2, j, this.seasonMatrix, this.iterResidualMatrix,
				this.cResidual, arCoef, maCoef, sigma2);
		}
		newGradient[this.p + this.q][0] = this.multiPComputeMLE(3, 0, this.seasonMatrix, this.iterResidualMatrix,
			this.cResidual, arCoef, maCoef, sigma2);

		DenseMatrix gradient;
		if (newGradient.length == 0) {
			gradient = DenseMatrix.zeros(0, 0);
		} else {
			gradient = new DenseMatrix(newGradient);
		}

		return gradient;
	}

	@Override
	public double f(DenseMatrix coef) {
		if (coef.numRows() != p + q + 1) {
			throw new AkIllegalDataException("Coef is not comparable with the model.");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}
		double sigma2 = coef.get(this.p + this.q, 0);

		double logLikelihood = this.multiComputeMLE(seasonMatrix, cResidual, arCoef, maCoef, sigma2);
		super.residual = TsMethod.seasonMatrix2Array(this.iterResidualMatrix);

		return logLikelihood;
	}
}
