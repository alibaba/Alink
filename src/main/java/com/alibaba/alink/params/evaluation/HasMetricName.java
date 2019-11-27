package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Metric name in evaluation.
 */
public interface HasMetricName<T> extends WithParams<T> {
	ParamInfo <String> METRIC_NAME = ParamInfoFactory
		.createParamInfo("metricName", String.class)
		.setDescription("metric name in evaluation")
		.setRequired()
		.build();

	default String getMetricName() {
		return get(METRIC_NAME);
	}

	default T setMetricName(String value) {
		return set(METRIC_NAME, value);
	}
}
