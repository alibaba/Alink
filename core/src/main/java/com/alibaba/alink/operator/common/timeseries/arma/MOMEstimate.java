package com.alibaba.alink.operator.common.timeseries.arma;

import com.alibaba.alink.operator.common.timeseries.TsMethod;

import java.util.ArrayList;

/**
 * Yule-Walker method.
 */
public class MOMEstimate extends ArmaEstimate {

	public MOMEstimate() {}

	@Override
	public void compute(double[] data, int p, int q, int ifIntercept) {
		this.mean = TsMethod.mean(data);
		double[] data1 = data.clone();
		//Let E(X)=0
		for (int i = 0; i < data.length; i++) {
			data1[i] = data1[i] - this.mean;
		}
		//AR
		if (q == 0 && p != 0) {
			ArrayList <double[]> coef = new MOMEstimate().momComputeAR(data1, p);
			this.arCoef = coef.get(0);
			this.maCoef = new double[] {0};
			this.variance = coef.get(1)[0];
			this.residual =  new double[data.length];
		}
		//MA
		if (p == 0 && q != 0) {
			ArrayList <double[]> coef = new MOMEstimate().momComputeMA(data1, q);
			this.maCoef = coef.get(0);
			this.arCoef = new double[] {0};
			this.variance = coef.get(1)[0];
			this.residual =  new double[data.length];
		}
		//ARMA
		if (p != 0 && q != 0) {
			ArrayList <double[]> coef = new MOMEstimate().momComputeARMA(data1, p, q);
			this.arCoef = coef.get(0);
			this.maCoef = coef.get(2);
			this.variance = coef.get(3)[0];
			CSSGradientTarget cssProblem = new CSSGradientTarget();
			this.css = cssProblem.computeRSS(data, new double[data.length], 0, arCoef, maCoef,
				intercept, 1,
				0);
			this.residual = cssProblem.iterResidual;
		}
	}

	/**
	 * result is a 2 row structure. 1st row contains all AR coefficients while 2rd row contains the variance of
	 * noise.
	 */
	private ArrayList <double[]> momComputeAR(double[] data, int p) {
		double[] acvf = TsMethod.acvf(data.clone(), p);
		double[][] coef = TsMethod.levinson(acvf);

		ArrayList <double[]> result = new ArrayList <double[]>();
		double[] a = new double[p];
		System.arraycopy(coef[p], 1, a, 0, p);
		result.add(a);
		result.add(new double[] {coef[0][p]});

		return result;
	}

	/**
	 * result is a 2 row structure. 1st row contains all AR coefficients while 2rd row contains the variance of
	 * noise.
	 */
	private ArrayList <double[]> momComputeMA(double[] data, int q) {
		//estimate order p
		int p = (int) Math.log(data.length);
		double[] acvf = TsMethod.acvf(data, p);
		double[][] arCoef = TsMethod.levinson(acvf);
		double[] phi = arCoef[p];
		phi[0] = -1;

		double[] acvfResidual = new double[q + 1];
		for (int h = 0; h <= q; h++) {
			double cor = 0;
			for (int j = 0; j <= p - h; j++) {
				cor = cor + phi[j] * phi[h + j];
			}
			acvfResidual[h] = cor / arCoef[0][p];
		}

		double[][] maCoef = TsMethod.levinson(acvfResidual);

		ArrayList <double[]> result = new ArrayList <double[]>();
		double[] a = new double[q];
		for (int i = 0; i < q; i++) {
			a[i] = -maCoef[q][i + 1];
		}
		result.add(a);
		result.add(new double[] {1 / maCoef[0][q]});

		return result;
	}

	private ArrayList <double[]> momComputeARMA(double[] data, int p, int q) {
		double[] allACVF = TsMethod.acvf(data, p + q);
		double[] arACVF = new double[p + 1];
		System.arraycopy(allACVF, q, arACVF, 0, arACVF.length);

		double[][] arCoef = TsMethod.levinson(arACVF);
		double[] phi = arCoef[p];
		phi[0] = -1;

		double[] maACVF = new double[q + 1];
		for (int h = 0; h < q + 1; h++) {
			double sum = 0;
			for (int i = 0; i <= p; i++) {
				for (int j = 0; j <= p; j++) {
					sum = sum + phi[i] * phi[j] * allACVF[Math.abs(h + i - j)];
				}
			}
			maACVF[h] = sum;
		}

		double[][] maCoef = TsMethod.levinson(maACVF);

		ArrayList <double[]> result = new ArrayList <double[]>();
		double[] a = new double[p];
		System.arraycopy(phi, 1, a, 0, p);
		result.add(a);
		result.add(new double[] {arCoef[0][p]});

		double[] b = new double[q];
		System.arraycopy(maCoef[q], 1, b, 0, p);

		result.add(b);
		result.add(new double[] {maCoef[0][q]});

		return result;
	}
}
