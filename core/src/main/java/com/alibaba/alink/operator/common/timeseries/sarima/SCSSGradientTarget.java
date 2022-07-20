package com.alibaba.alink.operator.common.timeseries.sarima;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;
import com.alibaba.alink.operator.common.timeseries.TsMethod;
import com.alibaba.alink.operator.common.timeseries.arma.CSSGradientTarget;

import java.util.ArrayList;

public class SCSSGradientTarget extends AbstractGradientTarget {
	private double[][] seasonMatrix;
	private double[] cResidual;
	public int p;
	public int q;
	public int length;
	public double css;
	double[][] iterResidualMatrix;

	public SCSSGradientTarget() {}

	/**
	 * for seasonal ARIMA, there is no intercept
	 */
	public void fit(double[][] seasonMatrix, double[] cResidual, ArrayList <double[]> initCoef, int season) {
		double[] arCoef = initCoef.get(0);
		double[] maCoef = initCoef.get(1);
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

		double[][] m = new double[arCoef.length + maCoef.length][1];

		for (int i = 0; i < arCoef.length; i++) {
			m[i][0] = arCoef[i];
		}
		for (int i = 0; i < maCoef.length; i++) {
			m[i + arCoef.length][0] = maCoef[i];
		}
		if (m.length == 0) {
			super.initCoef = DenseMatrix.zeros(0, 0);
		} else {
			super.initCoef = new DenseMatrix(m);
		}
	}

	double multiComputeRSS(double[][] seasonMatrix, double[] cResidual, double[] arCoef, double[] maCoef) {
		this.iterResidualMatrix = new double[seasonMatrix.length][seasonMatrix[0].length];
		for (int i = 0; i < seasonMatrix.length; i++) {
			for (int j = 0; j < seasonMatrix[0].length; j++) {
				this.iterResidualMatrix[i][j] = Double.MAX_VALUE;
			}
		}

		double sumRSS = 0;

		for (int i = 0; i < seasonMatrix.length; i++) {
			CSSGradientTarget cssGT = new CSSGradientTarget();
			double[] data = new TsMethod().seasonMatrixSplit(seasonMatrix, i);

			sumRSS += cssGT.computeRSS(data, cResidual, 0, arCoef, maCoef, 0, 1, 0);

			System.arraycopy(cssGT.iterResidual, 0, iterResidualMatrix[i], 0, data.length);
		}

		return sumRSS;
	}

	/**
	 * params: 1(AR), 2(MA) -- decide which part of model is computed
	 * coefLag: which lag in AR or MA is computed
	 */
	double multiPComputeRSS(int param, int coefLag, double[][] seasonMatrix, double[][] iterResidualMatrix,
							double[] arCoef, double[] maCoef) {

		double sumGradient = 0;
		CSSGradientTarget cssGT = new CSSGradientTarget();
		for (int i = 0; i < seasonMatrix.length; i++) {
			double[] data = new TsMethod().seasonMatrixSplit(seasonMatrix, i);
			sumGradient += cssGT.pComputeRSS(coefLag, data, iterResidualMatrix[i].clone(), 0, arCoef, maCoef, 0, 0,
				param);
		}

		return sumGradient;
	}

	@Override
	public DenseMatrix gradient(DenseMatrix coef, int iter) {
		if (coef.numRows() != p + q) {
			throw new AkIllegalDataException("coef is not comparable with the model.");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}

		//compute estimated residual
		this.multiComputeRSS(this.seasonMatrix, this.cResidual, arCoef, maCoef);

		double[][] newGradient = new double[p + q][1];

		//parameters gradient of AR
		for (int j = 0; j < this.p; j++) {
			//partial Inverse of ComputeRSS at sARCoef[j]
			newGradient[j][0] = this.multiPComputeRSS(1, j, seasonMatrix, iterResidualMatrix, arCoef, maCoef);
		}
		//parameters gradient of MA
		for (int j = 0; j < this.q; j++) {
			//partial Inverse of ComputeRSS at maCoef[j]
			newGradient[j + this.p][0] = this.multiPComputeRSS(2, j, seasonMatrix, iterResidualMatrix, arCoef, maCoef);
		}

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
		if (coef.numRows() != p + q) {
			throw new AkIllegalDataException("coef is not comparable with the model.");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}

		//compute estimated residual
		double css = this.multiComputeRSS(this.seasonMatrix, this.cResidual, arCoef, maCoef);

		super.setResidual(TsMethod.seasonMatrix2Array(this.iterResidualMatrix));

		return css;
	}
}
