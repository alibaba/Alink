package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;

public enum TuningRegressionMetric {
	MAE(RegressionMetrics.MAE),
	MSE(RegressionMetrics.MSE),
	RMSE(RegressionMetrics.RMSE),
	SAE(RegressionMetrics.SAE),
	MAPE(RegressionMetrics.MAPE),
	EXPLAINED_VARIANCE(RegressionMetrics.EXPLAINED_VARIANCE);

	private ParamInfo <Double> metricKey;

	TuningRegressionMetric(ParamInfo <Double> metricKey) {
		this.metricKey = metricKey;
	}

	public ParamInfo <Double> getMetricKey() {
		return metricKey;
	}
}
