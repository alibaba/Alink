package com.alibaba.alink.operator.common.timeseries;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.common.timeseries.arima.Arima;
import com.alibaba.alink.operator.common.timeseries.arima.ArimaModel;
import com.alibaba.alink.operator.common.timeseries.sarima.Sarima;
import com.alibaba.alink.operator.common.timeseries.sarima.SarimaModel;
import com.alibaba.alink.params.timeseries.HasEstmateMethod.EstMethod;
import com.alibaba.alink.params.timeseries.HasIcType.IcType;

import java.util.ArrayList;

/**
 * Seems some method of time serialize
 */
public class TsMethod {

	/**
	 * calculate the mean of the input data
	 */
	public static double mean(double[] data) {
		double sum = 0;
		for (double datum : data) {
			sum = sum + datum;
		}
		return sum / data.length;
	}

	/*
	 * Order is how many points forward for counting, ranging from 1 to order+1.
	 * acvf_h = cov(x_(h+t), x_t)
	 */
	public static double[] acvf(double[] data, int order) {
		if (order >= data.length) {
			throw new AkIllegalOperatorParameterException("Order for ComputeACVF must be smaller than length of data"
				+ ".");
		}

		double mean = mean(data);
		//order=p indicates ACVF.length=p+1, from cor(0) to cor(p)
		double[] acvf = new double[order + 1];
		for (int h = 0; h < acvf.length; h++) {
			double cor = 0;
			for (int i = h; i < data.length; i++) {
				cor = cor + (data[i] - mean) * (data[i - h] - mean);
			}
			acvf[h] = cor / (data.length - h);
		}
		return acvf;
	}

	/*
	 * Compute confidence interval using bartlett's formula.
	 * return <acf, confidence interval>
	 */
	public static ArrayList <double[]> acf(double[] data, int order) {
		double[] acvf = acvf(data, order);
		double[] acf = new double[acvf.length];
		ArrayList <double[]> result = new ArrayList <double[]>();

		acf[0] = 1;
		for (int i = 1; i < acf.length; i++) {
			acf[i] = acvf[i] / acvf[0];
		}
		result.add(acf);

		//compute confidence interval using bartlett's formula.
		double[] bartlettstd = new double[order + 1];
		for (int i = 0; i <= order; i++) {
			bartlettstd[i] = 0;
			for (int k = 1; k + i <= order; k++) {
				bartlettstd[i] = bartlettstd[i] + (acf[k + i] + acf[Math.abs(k - i)] - 2 * acf[k] * acf[i]) * (
					acf[k + i] + acf[Math.abs(k - i)] - 2 * acf[k] * acf[i]);
			}
		}
		result.add(bartlettstd);

		return result;
	}

	/*
	 * Compute PACF using Levinson Algorithm
	 */
	public static double[] computePACF(double[] data, int order) {
		double[] acvf = acvf(data, order);
		double[][] phi = levinson(acvf);
		double[] pacf = new double[acvf.length + 1];
		pacf[0] = 1;
		for (int i = 1; i < acvf.length; i++) {
			pacf[i] = phi[i][i];
		}
		//The last element is the std of PACF for choosing MA order.
		pacf[acvf.length] = Math.sqrt(1 / data.length);
		return pacf;
	}

	public static double[][] levinson(double[] acvf) {
		//coefficient [order+1][order+1]
		double[][] coef = new double[acvf.length][acvf.length];
		//variance of each step in Levinson algorithm
		double[] v = new double[acvf.length];
		//1th step
		v[0] = acvf[0];
		coef[1][1] = v[0] == 0 ? 0 : acvf[1] / v[0];
		v[1] = v[0] * (1 - coef[1][1] * coef[1][1]);
		//rest step
		for (int m = 2; m < acvf.length; m++) {
			double sum = 0;
			for (int j = 1; j < m; j++) {
				sum = sum + coef[m - 1][j] * acvf[m - j];
			}

			coef[m][m] = v[m - 1] == 0 ? 0 : (acvf[m] - sum) / v[m - 1];

			for (int i = 1; i < m; i++) {
				coef[m][i] = coef[m - 1][i] - coef[m][m] * coef[m - 1][m - i];
			}
			v[m] = v[m - 1] * (1 - coef[m][m] * coef[m][m]);
		}
		//coef[0] is estimated variance
		coef[0] = v;
		//return all coefficients in steps
		return coef;
	}

	private static double aicCompute(double logLike, int numParams) {
		return -2 * logLike + 2 * (numParams);
	}

	private static double bicCompute(double logLike, int numParams, int n) {
		return -2 * logLike + Math.log(n) * (numParams);
	}

	private static double hqicCompute(double logLike, int numParams, int n) {
		return -2 * logLike + Math.log(Math.log(n)) * (numParams) * 2;
	}

	public static double icCompute(double loglike, int numParams, int n, IcType ic, boolean ifAverage) {
		double xic = 0;

		switch (ic) {
			case AIC:
				xic = aicCompute(loglike, numParams);
				break;
			case BIC:
				xic = bicCompute(loglike, numParams, n);
				break;
			case HQIC:
				xic = hqicCompute(loglike, numParams, n);
				break;
		}

		if (ifAverage) {
			xic = xic / n;
		}
		return xic;
	}

	/**
	 * upperBound: at least 2
	 * ic: information criteria, "AIC", "BIC", "HQIC"
	 * estMethod: estimation method of coefficients
	 */
	public static ArimaModel stepWiseOrderSelect(double[] data,
												 int maxOrder,
												 EstMethod estMethod,
												 IcType ic, int d,
												 boolean ifAverage) {

		ArimaModel bestARIMA = null;
		double bestIC = Double.MAX_VALUE;

		int ifIntercept = d > 0 ? 0 : 1;

		//first step: (2,2),(0,0),(1,0),(0,1)
		//select the best model among the following model.
		//todo but is there any dependancy to choose like that?
		int[][] allOrder = {{0, d, 0}, {0, d, 1}, {1, d, 0}, {2, d, 2}};
		for (int[] anAllOrder : allOrder) {
			if (data.length - anAllOrder[0] - anAllOrder[1] < anAllOrder[0] + anAllOrder[2] + (anAllOrder[1] > 0
				? 0 : 1)) {
				continue;
			}

			ArimaModel arima = Arima.fit(data, anAllOrder[0],
				anAllOrder[1], anAllOrder[2], estMethod);

			if (arima.arma.estimate.warn != null) {
				continue;
			}

			int numParams = arima.p + arima.q + arima.ifIntercept;
			if (EstMethod.CssMle == arima.estMethod) {
				numParams += 1;
			}

			double xic = icCompute(arima.arma.estimate.logLikelihood, numParams, data.length - arima.p, ic, ifAverage);
			if (xic < bestIC) {
				bestIC = xic;
				bestARIMA = arima;
			}
		}

		if (bestIC == Double.MAX_VALUE) {
			if (bestARIMA.arma.estimate.warn == null) {
				bestARIMA.arma.estimate.warn = new ArrayList <String>();
			}
			bestARIMA.arma.estimate.warn.add("4");
		}

		if (maxOrder == 2 && bestARIMA.p == 2 && bestARIMA.q == 2) {
			return bestARIMA;
		}

		//step 2: find optimal model inside the boundary
		for (int i = 0; i < 50; i++) {
			int[] initOrder = new int[] {bestARIMA.p, d, bestARIMA.q};
			OneTimeSearch ots = new OneTimeSearch();
			ots.stepVariation(data, initOrder, maxOrder, ic, estMethod, ifIntercept, ifAverage);
			if (ots.xic >= bestIC) {
				break;
			}

			if (ots.xic < bestIC) {
				bestIC = ots.xic;
				bestARIMA = ots.arima;
			}
		}

		bestARIMA.ic = bestIC;
		return bestARIMA;
	}

	/**
	 * upperBound: at least 2
	 * ic: information criteria, "AIC", "BIC", "HQIC"
	 * estMethod: estimation method of coefficients
	 */
	public static SarimaModel seasonalStepWiseOrderSelect(double[] data,
														  int maxOrder,
														  int maxSeasonalOrder,
														  EstMethod estMethod,
														  IcType ic, int d,
														  int sD,
														  int seasonalPeriod,
														  boolean ifAverage) {
		if (seasonalPeriod == 1) {
			ArimaModel model = stepWiseOrderSelect(data,
				maxOrder,
				estMethod,
				ic, d,
				ifAverage);
			SarimaModel sModel = new SarimaModel(model.p, model.d, model.q, -1, -1, -1, estMethod, model.ifIntercept,
				seasonalPeriod);
			sModel.arima = model;
			sModel.warn = model.arma.estimate.warn;
			return sModel;
		}

		SarimaModel bestSARIMA = null;
		double bestIC = Double.MAX_VALUE;

		int ifIntercept = d > 0 ? 0 : 1;

		//step 1: (2,2),(0,0),(1,0),(0,1) x (1,1),(0,0),(1,0),(0,1)
		int[][] allOrder = {{0, d, 0}, {0, d, 1}, {1, d, 0}, {2, d, 2}};
		int[][] allSeasonOrder = {{0, sD, 0}, {0, sD, 1}, {1, sD, 0}, {1, sD, 1}};
		for (int i = 0; i < 4; i++) {
			int p = allOrder[i][0];
			int d1 = allOrder[i][1];
			int q = allOrder[i][2];
			int sP = allSeasonOrder[i][0];
			int sD1 = allSeasonOrder[i][1];
			int sQ = allSeasonOrder[i][2];

			//sarima data limit.
			if (data.length - sD1 * seasonalPeriod - d1 - p < p + q + sP + sQ + ifIntercept) {
				continue;
			}

			SarimaModel sarima = Sarima.fit(data, p, d1, q, sP, sD1, sQ,
				estMethod, ifIntercept, seasonalPeriod);

			if (sarima.warn != null) {
				continue;
			}

			int numParams = sarima.p + sarima.q + sarima.sP + sarima.sQ + ifIntercept;
			if (EstMethod.CssMle == sarima.estMethod) {
				numParams += 1;
			}

			double xic = icCompute(sarima.arima.arma.estimate.logLikelihood,
				numParams, sarima.sampleSize, ic,
				ifAverage);
			if (xic < bestIC) {
				bestIC = xic;
				bestSARIMA = sarima;
			}
		}

		if (bestIC == Double.MAX_VALUE) {
			if (bestSARIMA.warn == null) {
				bestSARIMA.warn = new ArrayList <String>();
			}
			bestSARIMA.warn.add("4");
		}

		if (maxOrder == 2 && bestSARIMA.p == 2 && bestSARIMA.q == 2) {
			return bestSARIMA;
		}

		//step 2: find optimal model inside the boundary
		for (int i = 0; i < 50; i++) {

			int[] initOrder = new int[] {bestSARIMA.p, d, bestSARIMA.q};
			int[] initSeasonOrder = new int[] {bestSARIMA.sP, sD, bestSARIMA.sQ};

			OneTimeSearch ots = new OneTimeSearch();
			ots.seasonalStepVariation(data,
				initOrder,
				initSeasonOrder,
				seasonalPeriod,
				maxOrder,
				maxSeasonalOrder,
				ic,
				estMethod,
				ifIntercept,
				ifAverage);
			if (ots.xic >= bestIC) {
				break;
			}

			if (ots.xic < bestIC) {
				bestIC = ots.xic;
				bestSARIMA = ots.sarima;
			}
		}

		bestSARIMA.ic = bestIC;
		return bestSARIMA;
	}

	/**
	 * difference begin at d
	 */
	public static double[] diff(int startidx, double[] data) {
		double[] diffData = new double[data.length];
		for (int i = startidx; i < data.length; i++) {
			diffData[i] = data[i] - data[i - 1];
		}
		return diffData;
	}

	/**
	 * change seasonally differenced prediction back to the original level
	 */
	public static double[] diffReduction(double[] data, double[] outcome) {
		double[] outcomeR = new double[outcome.length];
		outcomeR[0] = outcome[0] + data[data.length - 1];
		for (int i = 1; i < outcome.length; i++) {
			outcomeR[i] = outcome[i] + outcomeR[i - 1];
		}
		return outcomeR;
	}

	/**
	 * seasonally difference begin at seasonalPeriod*init
	 */
	public static double[] seasonDiff(int init, int seasonalPeriod, double[] data) {
		double[] diffData = new double[data.length];
		for (int i = seasonalPeriod * init; i < data.length; i++) {
			diffData[i] = data[i] - data[i - seasonalPeriod];
		}
		return diffData;
	}

	public static double[][] seasonArray2Matrix(double[] array, int season) {
		if (season < 1) {
			throw new AkIllegalOperatorParameterException("Season must > 0");
		}

		int ifAddOne = 0;
		if (array.length % season != 0) {
			ifAddOne = 1;
		}

		double[][] separateData = new double[season][(array.length / season) + ifAddOne];
		for (int i = 0; i < separateData.length; i++) {
			for (int j = 0; j < separateData[0].length; j++) {
				separateData[i][j] = Double.MAX_VALUE;
			}
		}

		for (int i = 0; i < array.length; i++) {
			separateData[i % season][i / season] = array[i];
		}

		return separateData;
	}

	public static double[] seasonMatrix2Array(double[][] matrix) {
		int season = matrix.length;

		int count = 0;
		for (int i = 0; i < season; i++) {
			if (matrix[i][matrix[0].length - 1] == Double.MAX_VALUE) {
				count += 1;
			}
		}
		int length = matrix.length * matrix[0].length;
		double[] array = new double[length - count];
		for (int i = 0; i < array.length; i++) {
			array[i] = matrix[i % season][i / season];
		}

		return array;
	}

	/**
	 * choose one row int seasonMatrix
	 */
	public double[] seasonMatrixSplit(double[][] matrix, int row) {
		int length = matrix[row].length;
		if (matrix[row][length - 1] == Double.MAX_VALUE) {
			length -= 1;
		}

		double[] array = new double[length];
		for (int i = 0; i < length; i++) {
			array[i] = matrix[row][i];
		}

		return array;
	}

	static class OneTimeSearch {
		ArimaModel arima;
		SarimaModel sarima;
		double xic;

		OneTimeSearch() {
		}

		void stepVariation(double[] data, int[] initOrder, int maxOrder, IcType ic,
						   EstMethod estMethod, int ifIntercept, boolean ifAverage) {
			ArrayList <ArimaModel> allModel = new ArrayList <ArimaModel>();

			double[] icResult = new double[7];
			for (int i = 0; i < icResult.length; i++) {
				icResult[i] = Double.MAX_VALUE;
			}

			int[][] allOrder = new int[7][3];
			for (int i = 0; i < allOrder.length; i++) {
				allOrder[i] = initOrder.clone();
			}
			//p+-1,q
			allOrder[0][0] -= 1;
			allOrder[1][0] += 1;
			//p,q+-1
			allOrder[2][2] -= 1;
			allOrder[3][2] += 1;
			//p+-1,q+-1
			allOrder[4][0] -= 1;
			allOrder[4][2] -= 1;
			allOrder[5][0] += 1;
			allOrder[5][2] += 1;

			for (int i = 0; i < allOrder.length; i++) {

				if (allOrder[i][0] > maxOrder || allOrder[i][2] > maxOrder
					|| allOrder[i][0] < 0 || allOrder[i][2] < 0) {
					allModel.add(null);
					continue;
				}

				int p = allOrder[i][0];
				int d = allOrder[i][1];
				int q = allOrder[i][2];

				if (data.length - p - d < p + q + ifIntercept) {
					allModel.add(null);
					continue;
				}

				if (i == allOrder.length - 1) {
					ArimaModel m;
					if (ifIntercept == 0) {
						m = Arima.fit(data, p, d, q, estMethod, 1);
					} else {
						m = Arima.fit(data, p, d, q, estMethod, 0);
					}
					int numParams = m.p + m.q + m.ifIntercept;
					if (EstMethod.CssMle == m.estMethod) {
						numParams += 1;
					}
					icResult[i] = icCompute(m.arma.estimate.logLikelihood, numParams, data.length - Math.max(m.p, m.q),
						ic,
						ifAverage);
					allModel.add(m);
					continue;
				}

				//compare between model with intercept and without intercept
				ArimaModel m = Arima.fit(data, p, d, q, estMethod, ifIntercept);
				if (m.arma.estimate.warn != null) {
					allModel.add(null);
					continue;
				}
				int numParams = m.p + m.q + m.ifIntercept;
				if (EstMethod.CssMle == m.estMethod) {
					numParams += 1;
				}
				icResult[i] = icCompute(m.arma.estimate.logLikelihood, numParams, data.length - m.p, ic, ifAverage);

				allModel.add(m);

			}

			this.xic = Double.MAX_VALUE;
			for (int i = 0; i < icResult.length; i++) {
				double xic = icResult[i];
				if (xic < this.xic) {
					this.xic = xic;
					this.arima = allModel.get(i);
				}
			}
		}

		void seasonalStepVariation(double[] data,
								   int[] initOrder,
								   int[] initSeasonOrder,
								   int seasonalPeriod,
								   int maxOrder,
								   int maxSeasonalOrder,
								   IcType ic,
								   EstMethod estMethod,
								   int ifIntercept,
								   boolean ifAverage) {
			ArrayList <SarimaModel> allModel = new ArrayList <SarimaModel>();

			double[] icResult = new double[13];
			for (int i = 0; i < icResult.length; i++) {
				icResult[i] = Double.MAX_VALUE;
			}

			int[][] allOrder = new int[13][3];
			int[][] allSeasonOrder = new int[13][3];
			for (int i = 0; i < allOrder.length; i++) {
				allOrder[i] = initOrder.clone();
			}
			for (int i = 0; i < allSeasonOrder.length; i++) {
				allSeasonOrder[i] = initSeasonOrder.clone();
			}
			//p+-1,q or sP+-1, sQ
			allOrder[0][0] -= 1;
			allOrder[1][0] += 1;
			allSeasonOrder[2][0] -= 1;
			allSeasonOrder[3][0] += 1;
			//p,q+-1 or sP, sQ+-1
			allOrder[4][2] -= 1;
			allOrder[5][2] += 1;
			allSeasonOrder[6][2] -= 1;
			allSeasonOrder[7][2] += 1;
			//p+-1,q+-1 or sP+-1,sQ+-1
			allOrder[8][0] -= 1;
			allOrder[8][2] -= 1;
			allOrder[9][0] += 1;
			allOrder[9][2] += 1;
			allSeasonOrder[10][0] -= 1;
			allSeasonOrder[10][2] -= 1;
			allSeasonOrder[11][0] += 1;
			allSeasonOrder[11][2] += 1;

			for (int i = 0; i < allOrder.length; i++) {
				if (allOrder[i][0] > maxOrder || allOrder[i][2] > maxOrder
					|| allOrder[i][0] < 0 || allOrder[i][2] < 0
					|| allSeasonOrder[i][0] > maxSeasonalOrder || allSeasonOrder[i][2] > maxSeasonalOrder
					|| allSeasonOrder[i][0] < 0 || allSeasonOrder[i][2] < 0) {
					allModel.add(null);
					continue;
				}
				int p = allOrder[i][0];
				int d = allOrder[i][1];
				int q = allOrder[i][2];
				int sp = allSeasonOrder[i][0];
				int sd = allSeasonOrder[i][1];
				int sq = allSeasonOrder[i][2];
				if (i == allOrder.length - 1) {
					SarimaModel m = null;
					if (ifIntercept == 1) {
						m = Sarima.fit(data, p, d, q, sp, sd, sq, estMethod, 0, seasonalPeriod);
					} else {
						m = Sarima.fit(data, p, d, q, sp, sd, sq, estMethod, 1, seasonalPeriod);
					}

					int numParams = m.p + m.q + m.sP + m.sQ + m.ifIntercept;
					if (EstMethod.CssMle == m.estMethod) {
						numParams += 1;
					}
					icResult[i] = icCompute(m.arima.arma.estimate.logLikelihood,
						numParams, m.sampleSize, ic, ifAverage);
					allModel.add(m);
					continue;
				}

				//compare between model with intercept and without intercept
				SarimaModel m = Sarima.fit(data, p, d, q, sp, sd, sq, estMethod, ifIntercept, seasonalPeriod);

				if (m.warn != null) {
					allModel.add(null);
					continue;
				}
				int numParams = m.p + m.q + m.ifIntercept + m.sP + m.sQ;
				if (EstMethod.CssMle == m.estMethod) {
					numParams += 1;
				}
				icResult[i] = icCompute(m.arima.arma.estimate.logLikelihood,
					numParams, m.sampleSize, ic, ifAverage);

				allModel.add(m);

			}

			this.xic = Double.MAX_VALUE;
			for (int i = 0; i < icResult.length; i++) {
				double xic = icResult[i];
				if (xic < this.xic) {
					this.xic = xic;
					this.sarima = allModel.get(i);
				}
			}
		}

	}

}
