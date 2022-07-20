package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.timeseries.AbstractGradientTarget;

import java.util.ArrayList;

public class CSSGradientTarget extends AbstractGradientTarget {
	public double[] data;
	public double[] cResidual;
	public int p;
	public int q;
	public int ifIntercept;
	public int optOrder;
	public int type;
	public double[] iterResidual;
	public double css;

	public CSSGradientTarget() {
	}

	public void fit(ArrayList <double[]> init, double[] data, double[] cResidual, int optOrder, int type,
					int ifIntercept) {
		double[] arCoef = init.get(0);
		double[] maCoef = init.get(1);
		double intercept = init.get(2)[0];
		this.p = arCoef.length;
		this.q = maCoef.length;
		this.data = data;
		this.cResidual = cResidual;
		this.type = type;
		this.optOrder = optOrder;
		double[][] a = new double[data.length][1];
		for (int q = 0; q < data.length; q++) {
			a[q][0] = data[q];
		}
		super.setX(new DenseMatrix(a));

		double[][] m;
		this.ifIntercept = ifIntercept;
		if (ifIntercept == 0) {
			m = new double[arCoef.length + maCoef.length][1];
		} else {
			m = new double[arCoef.length + maCoef.length + 1][1];
			m[m.length - 1][0] = intercept;
		}
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

	/**
	 * Compute residual at t lag using ARMA formula: sARCoef*data-maCoef*residual-intercept.
	 */
	public double oneRSS(int t, double[] data, double[] residual, double[] arCoef, double[] maCoef, double intercept) {
		//sum of AR part
		double sumAR = data[t];
		for (int j = 0; j < arCoef.length; j++) {
			sumAR = sumAR - arCoef[j] * data[t - j - 1];
		}

		//sum of MA part
		double sumMA = 0;
		int iterLenght = maCoef.length;
		if (t < maCoef.length) {
			iterLenght = t;
		}

		for (int j = 0; j < iterLenght; j++) {
			sumMA = sumMA + maCoef[j] * residual[t - j - 1];
		}

		return sumAR - sumMA - intercept;
	}

	/**
	 * Compute conditional residual sum of square in second step.
	 * Use residual computed from high order AR model in first step.
	 */
	public double computeRSS(double[] data, double[] residual, int optOrder, double[] arCoef, double[] maCoef,
							 double intercept, int type, int ifIntercept) {
		this.iterResidual = residual.clone();

		int m = arCoef.length + optOrder;
		if (type == 1) {
			m = arCoef.length;
		}

		double rss = 0;
		for (int t = m; t < data.length; t++) {
			double oneRSS = ifIntercept == 1 ?
				this.oneRSS(t, data, this.iterResidual, arCoef, maCoef, intercept)
				: this.oneRSS(t, data, this.iterResidual, arCoef, maCoef, 0);

			//change
			if (type == 1) {
				this.iterResidual[t] = oneRSS;
			}

			rss = rss + oneRSS * oneRSS;
		}

		return rss;
	}

	/**
	 * Compute partial Inverse of CSS function.
	 * type is for 2 kinds of initiations for CSS.
	 * param decides whether the partial Inverse is for AR coefficients, MA coefficients or intercept. 1 for AR, 2
	 * for MA, 3 for intercept
	 * j decides which coefficient is computed for, from 0 to order-1
	 */
	public double pComputeRSS(int j, double[] data, double[] residual, int optOrder, double[] arCoef, double[] maCoef,
							  double intercept, int type, int param) {

		int m = Math.max(arCoef.length + optOrder, maCoef.length + optOrder);
		if (type == 1) {
			m = Math.max(arCoef.length, maCoef.length);
		}

		double dF = 0;
		//AR partial Inverse
		if (param == 1) {
			for (int t = m; t < data.length; t++) {
				dF = dF - 2 * this.oneRSS(t, data, residual, arCoef, maCoef, intercept) * data[t - j - 1];
			}
		}
		//MA partial Inverse
		if (param == 2) {
			for (int t = m; t < data.length; t++) {
				dF = dF - 2 * this.oneRSS(t, data, residual, arCoef, maCoef, intercept) * residual[t - j - 1];
			}
		}
		//intercept partial Inverse
		if (param == 3) {
			for (int t = m; t < data.length; t++) {
				dF = dF - 2 * this.oneRSS(t, data, residual, arCoef, maCoef, intercept);
			}
		}

		return dF;
	}

	@Override
	public DenseMatrix gradient(DenseMatrix coef, int iter) {
		if (coef.numRows() != p + q && coef.numRows() != p + q + 1) {
			throw new AkIllegalDataException("coef is not comparable with the model.");
		}

		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		double intercept;
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}
		if (this.ifIntercept == 1) {
			intercept = coef.get(p + q, 0);
		} else {
			intercept = 0;
		}
		//compute estimated residual
		this.computeRSS(this.data, this.cResidual, this.optOrder, arCoef, maCoef, intercept, this.type,
			this.ifIntercept);

		double[][] newGradient;
		if (ifIntercept == 1) {
			newGradient = new double[p + q + 1][1];
		} else {
			newGradient = new double[p + q][1];
		}

		//parameters gradient of AR
		for (int j = 0; j < this.p; j++) {
			//partial Inverse of ComputeRSS at sARCoef[j]
			newGradient[j][0] = this.pComputeRSS(j, data, this.iterResidual, this.optOrder, arCoef, maCoef, intercept,
				type, 1);
		}

		//parameters gradient of MA
		for (int j = 0; j < this.q; j++) {
			//partial Inverse of ComputeRSS at maCoef[j]
			newGradient[j + this.p][0] = this.pComputeRSS(j, data, this.iterResidual, this.optOrder, arCoef, maCoef,
				intercept, type, 2);
		}

		//gradient of intercept
		if (this.ifIntercept == 1) {
			newGradient[this.p + this.q][0] = this.pComputeRSS(0, data, this.iterResidual, this.optOrder, arCoef,
				maCoef, intercept, type, 3);
		}

		return new DenseMatrix(newGradient);
	}

	@Override
	public double f(DenseMatrix coef) {
		double[] arCoef = new double[p];
		double[] maCoef = new double[q];
		double intercept;
		for (int i = 0; i < this.p; i++) {
			arCoef[i] = coef.get(i, 0);
		}
		for (int i = 0; i < this.q; i++) {
			maCoef[i] = coef.get(i + this.p, 0);
		}
		if (this.ifIntercept == 1) {
			intercept = coef.get(p + q, 0);
		} else {
			intercept = 0;
		}
		this.css = this.computeRSS(this.data, this.cResidual, this.optOrder, arCoef, maCoef, intercept, this.type,
			this.ifIntercept);
		super.residual = this.iterResidual;

		return this.css;
	}
}
