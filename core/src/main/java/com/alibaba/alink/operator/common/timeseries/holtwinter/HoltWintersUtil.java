package com.alibaba.alink.operator.common.timeseries.holtwinter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.Arrays;

/**
 * The base operation of holt winters.
 */
public class HoltWintersUtil {

	/**
	 * calc loss.
	 *
	 * @param hyperParams  alpha, beta, gamma.
	 * @return double
	 */
	public static double calcLoss(DenseVector data,
								  Double levelStart,
								  Double trendStart,
								  DenseVector seasonalStart,
								  DenseVector hyperParams,
								  Boolean seasonal,
								  int frequency) {
		//initial level, trend and seasonalPeriod.
		Tuple3 <Double, Double, double[]> initialArrayData = Tuple3.of(levelStart, trendStart,
			seasonalStart.getData());
		return HoltWintersUtil.holtWintersTrain(data.getData(),
			initialArrayData, hyperParams.getData(), Tuple2.of(seasonal, frequency));
	}

	/**
	 * calc grad.
	 *
	 * @param initialParams coefs.
	 * @param data          data.
	 * @param epsilon       epsilon.
	 * @param lowerBound    lower bound.
	 * @param upperBound    upper bound.
	 * @return
	 */
	public static DenseVector calcGrad(DenseVector initialParams,
									   Boolean seasonal,
									   int frequency,
									   Double levelStart,
									   Double trendStart,
									   DenseVector seasonalStart,
									   Vector data,
									   DenseVector epsilon,
									   DenseVector lowerBound,
									   DenseVector upperBound) {
		Tuple3 <Double, Double, double[]> initialArrayData = Tuple3.of(levelStart, trendStart,
			seasonalStart.getData());

		int coefSize = initialParams.size();
		if (epsilon == null) {
			double[] epsilonData = new double[coefSize];
			Arrays.fill(epsilonData, 0.001);
			epsilon = new DenseVector(epsilonData);
		}
		DenseVector grad = new DenseVector(coefSize);
		//get coefs(alpha, beta gamma) to coefsArray.
		double[] coefsArray = initialParams.getData().clone();
		//copy.
		double[] coefsCalcCopy = coefsArray.clone();
		double epsused;
		for (int i = 0; i < coefSize; i++) {
			coefsArray[i] = coefsCalcCopy[i] + epsilon.get(i);
			if (coefsArray[i] > upperBound.get(i)) {
				epsused = upperBound.get(i) - coefsCalcCopy[i];
				coefsArray[i] = upperBound.get(i);
			} else {
				epsused = epsilon.get(i);
			}
			double upDelta = HoltWintersUtil.holtWintersTrain(((DenseVector) data).getData(),
				initialArrayData, coefsArray, Tuple2.of(seasonal, frequency));

			coefsArray[i] = coefsCalcCopy[i] - epsilon.get(i);
			if (coefsArray[i] < lowerBound.get(i)) {
				coefsArray[i] = lowerBound.get(i);
				epsused += coefsCalcCopy[i] - coefsArray[i];
			} else {
				epsused += epsilon.get(i);
			}
			double lowDelta = HoltWintersUtil.holtWintersTrain(((DenseVector) data).getData(),
				initialArrayData, coefsArray, Tuple2.of(seasonal, frequency));
			grad.set(i, (upDelta - lowDelta) / epsused);
		}
		return grad;
	}

	/**
	 * @param predictNum         predict number
	 * @param initialHyperParams alpha, beta, gamma
	 * @param seasonal           if seasonal or not
	 * @param frequency          frequency
	 * @param levelStart         level start
	 * @param trendStart         trend start
	 * @param seasonalStart      seasonal start
	 * @return DenseVector
	 */
	public static DenseVector holtWintersForecast(int predictNum,
												  double[] initialHyperParams,
												  boolean seasonal,
												  int frequency,
												  Double levelStart,
												  Double trendStart,
												  double[] seasonalStart) {
		return new DenseVector(
			holtWinters(null, initialHyperParams, seasonal, frequency,
				levelStart, trendStart, seasonalStart, predictNum,
				true, false, false).f1);
	}

	/**
	 * get initial levelStart, trendStart, seasonalStart.
	 *
	 * @param data               data.
	 * @param initialHyperParams coefs.
	 * @param seasonal           do seasonal or not.
	 * @param frequency          frequency.
	 * @param levelStart         level start.
	 * @param trendStart         trend start.
	 * @param seasonalStart      seasonal start.
	 * @return <level start,trend start,  seasonal start>
	 */
	public static Tuple3 <Double, Double, double[]> calculateInitialData(double[] data,
																		 double[] initialHyperParams,
																		 boolean seasonal,
																		 int frequency,
																		 Double levelStart,
																		 Double trendStart,
																		 double[] seasonalStart) {
		return holtWinters(data, initialHyperParams, seasonal, frequency,
			levelStart, trendStart, seasonalStart,
			0, false, true,
			false).f2;
	}

	//this is used in loss and grad calculation.
	static double holtWintersTrain(double[] data,
								   Tuple3 <Double, Double, double[]> initialData,
								   double[] initialHyperParams,
								   Tuple2 <Boolean, Integer> params) {

		return holtWinters(data, initialHyperParams, params.f0, params.f1,
			initialData.f0, initialData.f1, initialData.f2).f0;
	}

	public static Tuple3 <double[], double[], double[]> decompose(double[] data,
																  double[] initialHyperParams,
																  Tuple2 <Boolean, Integer> params,
																  Tuple3 <Double, Double, double[]> initialData) {
		return holtWinters(data,
			initialHyperParams, params.f0, params.f1,
			initialData.f0, initialData.f1, initialData.f2,
			0, false, false, true).f3;
	}

	private static Tuple2 <Double, double[]> holtWinters(double[] data,
														 double[] initialHyperParams,
														 boolean seasonal,
														 int frequency,
														 Double levelStart,
														 Double trendStart,
														 double[] seasonalStart) {
		Tuple4 <Double, double[], Tuple3 <Double, Double, double[]>, Tuple3 <double[], double[], double[]>> res =
			holtWinters(data, initialHyperParams, seasonal, frequency,
				levelStart, trendStart, seasonalStart, 0,
				false, false, false);
		return Tuple2.of(res.f0, res.f1);
	}

	//this is used in forecast, so the initial data are the last data in level, trend and seasonalPeriod.
	private static Tuple4 <Double, double[],
		Tuple3 <Double, Double, double[]>,
		Tuple3 <double[], double[], double[]>> holtWinters(double[] data,
														   double[] initialHyperParams,
														   boolean seasonal,
														   int frequency,
														   Double levelStart,
														   Double trendStart,
														   double[] seasonalStart,
														   int forecastStep,
														   boolean isForecast,
														   boolean calculateInitialData,
														   boolean doDecompose) {
		double newa = 0;
		double newb = 0;
		double[] news = new double[0];

		double alpha = initialHyperParams[0];
		Double beta = null;
		Double gamma = null;
		if (initialHyperParams.length >= 2) {
			beta = initialHyperParams[1];
			if (initialHyperParams.length == 3) {
				gamma = initialHyperParams[2];
			}
		}
		double sse = 0;
		double res = 0;
		double xhat = 0;
		double stmp = 0;
		int length;
		if (isForecast) {
			length = forecastStep;
		} else {
			length = data.length;
		}
		boolean dotrend = beta != null;
		boolean doseasonal = gamma != null;
		//initialize news.
		if (calculateInitialData) {
			if (dotrend && doseasonal) {
				news = new double[seasonalStart.length];
			}
		}
		//initialize the array. trend and seasonalPeriod will be initialized when needed.
		double[] level = new double[length + 1];
		double[] trend = new double[1];
		double[] season = new double[1];
		//initialization
		level[0] = levelStart;
		if (dotrend) {//trend
			trend = new double[length + 1];
			trend[0] = trendStart;
			if (doseasonal) {//seasonalPeriod
				season = new double[length + frequency];
				System.arraycopy(seasonalStart, 0, season, 0, frequency);
			}
		}

		//fitted data is used for holt winters prediction.
		double[] fittedData = new double[length];
		for (int i = 0; i < length; i++) {
			int s0 = i + frequency;
			xhat = level[i];//check whether it is i or i+1.
			if (dotrend) {
				xhat += trend[i];
			}
			if (doseasonal) {
				stmp = season[s0 - frequency];
			} else {
				if (seasonal) {
					stmp = 0;
				} else {
					stmp = 1;
				}
			}

			if (seasonal) {
				xhat += stmp;
			} else {
				xhat *= stmp;
			}
			//calc sse.
			//seems xhat is the prediction of the input data.
			fittedData[i] = xhat;

			double dataI;
			if (isForecast) {
				dataI = fittedData[i];
			} else {
				dataI = data[i];
			}
			res = dataI - xhat;
			sse += Math.pow(res, 2);
			//estimate of level
			double tempValue;
			if (dotrend) {
				tempValue = (1 - alpha) * (level[i] + trend[i]);
			} else {
				tempValue = (1 - alpha) * level[i];
			}
			if (seasonal) {//this is distinct add and multiple.
				level[i + 1] = alpha * (dataI - stmp) + tempValue;
			} else {
				level[i + 1] = alpha * (dataI / stmp) + tempValue;
			}
			//estimate of trend
			if (dotrend) {
				trend[i + 1] = beta * (level[i + 1] - level[i]) + (1 - beta) * trend[i];
			}
			//estimate of seasonal
			if (doseasonal) {
				if (seasonal) {
					season[s0] = gamma * (dataI - level[i + 1]) + (1 - gamma) * stmp;
				} else {
					season[s0] = gamma * (dataI / level[i + 1]) + (1 - gamma) * stmp;
				}
			}
		}
		if (calculateInitialData) {
			newa = level[length];
			if (dotrend) {
				newb = trend[length];
				if (doseasonal) {
					System.arraycopy(season, length, news, 0, frequency);
				}
			}
		}
		Tuple3 <double[], double[], double[]> decompose = null;
		if (doDecompose) {
			double[] decomposedLevel = new double[length];
			System.arraycopy(level, 1, decomposedLevel, 0, length);
			double[] decomposedTrend = new double[length];
			System.arraycopy(trend, 1, decomposedTrend, 0, length);
			double[] decomposedSeason = new double[length];
			System.arraycopy(season, frequency, decomposedSeason, 0, length);
			decompose = Tuple3.of(decomposedLevel, decomposedTrend, decomposedSeason);
		}
		return Tuple4.of(sse, fittedData, Tuple3.of(newa, newb, news), decompose);
	}

}
