package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Save the evaluation data for time series. The evaluation metrics include:
 *
 * <p>SST: Sum of Squared for Total, SST = sum(yi-y_hat)^2
 *
 * <p>SSE: Sum of Squares for Error, SSE = sum(yi-fi)^2
 *
 * <p>SSR: Sum of Squares for Regression, SSR = sum(fi_y_hat)^2
 *
 * <p>R^2: Coefficient of Determination, R2 = 1 - SSE/SST
 *
 * <p>R: Multiple CorrelationBak Coeffient, R = sqrt(R2)
 *
 * <p>MSE: Mean Squared Error, MSE = SSE/N
 *
 * <p>RMSE: Root Mean Squared Error, RMSE = sqrt(MSE)
 *
 * <p>SAE/SAD: Sum of Absolute Error/Difference, SAE = sum|fi-yi|
 *
 * <p>MAE/MAD: Mean Absolute Error/Difference, MAE = SAE/N
 *
 * <p>MAPE: Mean Absolute Percentage Error, MAPE = sum|(fi-yi)/yi|*100/N
 *
 * <p>Explained Variance: SSR / N
 */
public final class TimeSeriesMetricsSummary
	implements BaseMetricsSummary <TimeSeriesMetrics, TimeSeriesMetricsSummary> {
	/**
	 * Sum of y values.
	 */
	double ySumLocal;

	/**
	 * Sum of absolute y values.
	 */
	double aySumLocal;

	/**
	 * Sum of square of y values.
	 */
	double ySum2Local;

	/**
	 * Sum of prediction values.
	 */
	double predSumLocal;

	/**
	 * Sum of square of prediction values.
	 */
	double predSum2Local;

	/**
	 * Sum of square of errors.
	 */
	double sseLocal;

	/**
	 * Sum of absolute errors.
	 */
	double maeLocal;

	/**
	 * Sum of absolute percentage errors.
	 */
	double mapeLocal;

	/**
	 * Sum of symmetric absolute percentage errors.
	 */
	double smapeLocal;

	/**
	 * The count of samples.
	 */
	long total = 0L;

	@Override
	public TimeSeriesMetricsSummary merge(TimeSeriesMetricsSummary other) {
		if (null == other) {
			return this;
		}
		ySumLocal += other.ySumLocal;
		aySumLocal += other.aySumLocal;
		ySum2Local += other.ySum2Local;
		predSumLocal += other.predSumLocal;
		predSum2Local += other.predSum2Local;
		sseLocal += other.sseLocal;
		maeLocal += other.maeLocal;
		mapeLocal += other.mapeLocal;
		smapeLocal += other.smapeLocal;
		total += other.total;
		return this;
	}

	@Override
	public TimeSeriesMetrics toMetrics() {
		Params params = new Params();
		params.set(TimeSeriesMetrics.SST, ySum2Local - ySumLocal * ySumLocal / total);
		params.set(TimeSeriesMetrics.SSE, sseLocal);
		params.set(TimeSeriesMetrics.SSR,
			predSum2Local - 2 * ySumLocal * predSumLocal / total + ySumLocal * ySumLocal / total);
		params.set(TimeSeriesMetrics.MSE, params.get(TimeSeriesMetrics.SSE) / total);
		params.set(TimeSeriesMetrics.RMSE, Math.sqrt(params.get(TimeSeriesMetrics.MSE)));
		params.set(TimeSeriesMetrics.SAE, maeLocal);
		params.set(TimeSeriesMetrics.MAE, params.get(TimeSeriesMetrics.SAE) / total);
		params.set(TimeSeriesMetrics.COUNT, (double) total);
		params.set(TimeSeriesMetrics.MAPE, mapeLocal * 100 / total);
		params.set(TimeSeriesMetrics.SMAPE, smapeLocal * 200 / total);
		params.set(TimeSeriesMetrics.ND, maeLocal / aySumLocal);
		params.set(TimeSeriesMetrics.Y_MEAN, ySumLocal / total);
		params.set(TimeSeriesMetrics.PREDICTION_MEAN, predSumLocal / total);
		params.set(TimeSeriesMetrics.EXPLAINED_VARIANCE, params.get(TimeSeriesMetrics.SSR) / total);

		return new TimeSeriesMetrics(params);
	}
}
