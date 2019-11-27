package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Save the evaluation data for regression. The evaluation metrics include:
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
public final class RegressionMetricsSummary implements BaseMetricsSummary<RegressionMetrics, RegressionMetricsSummary> {
    /**
     * Sum of label values.
     */
    double ySumLocal;

    /**
     * Sum of square of label values.
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
     * The count of samples.
     */
    long total = 0L;

    @Override
    public RegressionMetricsSummary merge(RegressionMetricsSummary other) {
        if (null == other) {
            return this;
        }
        ySumLocal += other.ySumLocal;
        ySum2Local += other.ySum2Local;
        predSumLocal += other.predSumLocal;
        predSum2Local += other.predSum2Local;
        sseLocal += other.sseLocal;
        maeLocal += other.maeLocal;
        mapeLocal += other.mapeLocal;
        total += other.total;
        return this;
    }

    @Override
    public RegressionMetrics toMetrics() {
        Params params = new Params();
        params.set(RegressionMetrics.SST, ySum2Local - ySumLocal * ySumLocal / total);
        params.set(RegressionMetrics.SSE, sseLocal);
        params.set(RegressionMetrics.SSR,
            predSum2Local - 2 * ySumLocal * predSumLocal / total + ySumLocal * ySumLocal / total);
        params.set(RegressionMetrics.R2, 1 - params.get(RegressionMetrics.SSE) / params.get(RegressionMetrics.SST));
        params.set(RegressionMetrics.R, Math.sqrt(params.get(RegressionMetrics.R2)));
        params.set(RegressionMetrics.MSE, params.get(RegressionMetrics.SSE) / total);
        params.set(RegressionMetrics.RMSE, Math.sqrt(params.get(RegressionMetrics.MSE)));
        params.set(RegressionMetrics.SAE, maeLocal);
        params.set(RegressionMetrics.MAE, params.get(RegressionMetrics.SAE) / total);
        params.set(RegressionMetrics.COUNT, (double)total);
        params.set(RegressionMetrics.MAPE, mapeLocal * 100 / total);
        params.set(RegressionMetrics.Y_MEAN, ySumLocal / total);
        params.set(RegressionMetrics.PREDICTION_MEAN, predSumLocal / total);
        params.set(RegressionMetrics.EXPLAINED_VARIANCE, params.get(RegressionMetrics.SSR) / total);

        return new RegressionMetrics(params);
    }
}
