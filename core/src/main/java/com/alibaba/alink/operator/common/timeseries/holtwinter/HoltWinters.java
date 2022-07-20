package com.alibaba.alink.operator.common.timeseries.holtwinter;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.timeseries.TimeSeriesUtils;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType.SeasonalType;

import java.util.Arrays;

/**
 * HoltWinters.
 */
public class HoltWinters {

	/**
	 * holt winters fit.
	 *
	 * @param data          time series data.
	 * @param frequency     time series frequency
	 * @param alpha         alpha
	 * @param beta          beta
	 * @param gamma         gamma
	 * @param doTrend       doTrend
	 * @param doSeasonal    doSeasonal
	 * @param seasonalType  seasonalType
	 * @param levelStart    levelStart
	 * @param trendStart    trendStart
	 * @param seasonalStart seasonalStart
	 * @return HoltWintersModel
	 */
	public static HoltWintersModel fit(double[] data,
									   int frequency,
									   double alpha,
									   double beta,
									   double gamma,
									   boolean doTrend,
									   boolean doSeasonal,
									   SeasonalType seasonalType,
									   Double levelStart,
									   Double trendStart,
									   double[] seasonalStart) {
		if (data == null) {
			throw new AkIllegalDataException("Data is null.");
		}

		for (double val : data) {
			if (Double.isNaN(val) || Double.isInfinite(val)) {
				return null;
			}
		}

		Tuple3 <Double, Double, double[]> starts = initStart(
			data, frequency, doTrend, doSeasonal, seasonalType,
			levelStart, trendStart, seasonalStart);

		levelStart = starts.f0;
		trendStart = starts.f1;
		seasonalStart = starts.f2;

		DenseVector coefs;
		try {
			coefs = train(data,
				frequency, alpha, beta, gamma,
				doTrend, doSeasonal, seasonalType,
				levelStart, trendStart, seasonalStart);
		} catch (Throwable ex) {
			for (int i = 0; i < data.length; i++) {
				data[i] += 1e-10;
			}
			coefs = train(data,
				frequency, alpha, beta, gamma,
				doTrend, doSeasonal, seasonalType,
				levelStart, trendStart, seasonalStart);
		}

		boolean isAddType = seasonalType == SeasonalType.ADDITIVE;

		Tuple3 <Double, Double, double[]> initialData = HoltWintersUtil.calculateInitialData(
			data, coefs.getData(), isAddType, frequency, levelStart, trendStart, seasonalStart);

		HoltWintersModel model = new HoltWintersModel();
		model.levelStart = initialData.f0;
		model.trendStart = initialData.f1;
		model.seasonalStart = initialData.f2;
		model.coefs = coefs.getData();
		model.seasonalType = seasonalType;
		model.frequency = frequency;

		return model;
	}

	/**
	 * int level start, trend start and seasonal start.
	 *
	 * @param frequency     time series frequency
	 * @param doTrend       doTrend
	 * @param doSeasonal    doSeasonal
	 * @param seasonalType  seasonalType
	 * @param levelStart    levelStart
	 * @param trendStart    trendStart
	 * @param seasonalStart seasonalStart
	 * @return <levelStart, trendStart, seasonalStart>
	 */
	private static Tuple3 <Double, Double, double[]> initStart(double[] data,
															   int frequency,
															   boolean doTrend,
															   boolean doSeasonal,
															   SeasonalType seasonalType,
															   Double levelStart,
															   Double trendStart,
															   double[] seasonalStart) {
		if (doSeasonal) {
			if (levelStart == null) {
				int count = 0;
				double sum = 0;
				for (int i = 0; i < data.length; i += frequency) {
					sum += data[i];
					count++;
				}
				levelStart = sum / count;
			}
			if (trendStart == null) {
				if (2 * frequency > data.length) {
					throw new AkIllegalDataException("need at least 2 periods to compute seasonal start values.");
				}
				trendStart = (sum(data, frequency, 2 * frequency) - sum(data, 0, frequency)) / Math.pow(frequency, 2);
			}
			if (seasonalStart == null) {
				seasonalStart = new double[frequency];
				if (seasonalType == SeasonalType.ADDITIVE) {
					for (int i = 0; i < frequency; i++) {
						seasonalStart[i] = data[i] - levelStart;
					}
				} else {
					for (int i = 0; i < frequency; i++) {
						seasonalStart[i] = data[i] / levelStart;
					}
				}
			}
		} else if (doTrend) {
			if (levelStart == null) {
				levelStart = data[0];
			}
			if (trendStart == null) {
				if (2 * frequency > data.length) {
					throw new AkIllegalDataException("need at least 2 periods to compute seasonal start values.");
				}
				if (seasonalType == SeasonalType.ADDITIVE) {
					trendStart = data[1] - data[0];
				} else {
					trendStart = data[1] / data[0];
				}
			}
		} else {
			if (levelStart == null) {
				levelStart = data[0];
			}
		}

		return Tuple3.of(levelStart, trendStart, seasonalStart);
	}

	/**
	 * calculate the optimized coefs.
	 *
	 * @param data          time series data.
	 * @param frequency     time series frequency
	 * @param alpha         alpha
	 * @param beta          beta
	 * @param gamma         gamma
	 * @param doTrend       doTrend
	 * @param doSeasonal    doSeasonal
	 * @param seasonalType  seasonalType
	 * @param levelStart    levelStart
	 * @param trendStart    trendStart
	 * @param seasonalStart seasonalStart
	 * @return DenseVector
	 */
	private static DenseVector train(double[] data,
									 int frequency,
									 double alpha,
									 double beta,
									 double gamma,
									 boolean doTrend,
									 boolean doSeasonal,
									 SeasonalType seasonalType,
									 Double levelStart,
									 Double trendStart,
									 double[] seasonalStart) {
		DenseVector initParams;

		if (doSeasonal) {
			initParams = new DenseVector(new double[] {alpha, beta, gamma});
		} else if (doTrend) {
			initParams = new DenseVector(new double[] {alpha, beta});
		} else {
			initParams = new DenseVector(new double[] {alpha});
		}

		int size = initParams.size();

		DenseVector lowerBound = DenseVector.zeros(size);

		DenseVector upperBound = DenseVector.ones(size);

		DenseVector epsilon = new DenseVector(size);
		Arrays.fill(epsilon.getData(), 0.001);

		return solve(lowerBound, upperBound, initParams,
			levelStart, trendStart, new DenseVector(seasonalStart),
			new DenseVector(data), seasonalType == SeasonalType.ADDITIVE,
			frequency, epsilon);
	}

	/**
	 * solve holtwinters.
	 *
	 * @param lowerBound  lowerBound.
	 * @param upperBound  upperBound.
	 * @param hyperParams alpha, beta, gamma.
	 * @param epsilon     epsilon
	 * @return DenseVector
	 */
	private synchronized static DenseVector solve(DenseVector lowerBound,
												  DenseVector upperBound,
												  DenseVector hyperParams,
												  Double levelStart,
												  Double trendStart,
												  DenseVector seasonalStart,
												  DenseVector data,
												  boolean seasonal,
												  int frequency,
												  DenseVector epsilon) {
		return TimeSeriesUtils.calcHoltWintersLBFGSB(lowerBound, upperBound, hyperParams,
			levelStart, trendStart, seasonalStart,
			data, seasonal, frequency, epsilon, 500, 1e-8);
	}

	/**
	 * sum of data from start to end.
	 *
	 * @param data  data
	 * @param start startPos
	 * @param end   endPos
	 * @return double
	 */
	private static double sum(double[] data, int start, int end) {
		double res = 0;
		for (int i = start; i < end; i++) {
			res += data[i];
		}
		return res;
	}
}
