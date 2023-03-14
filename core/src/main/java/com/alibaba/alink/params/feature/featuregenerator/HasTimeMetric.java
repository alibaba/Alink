package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasTimeMetric <T> extends WithParams<T> {
	ParamInfo <TimeMetric> TIME_METRIC = ParamInfoFactory
		.createParamInfo("timeMetric", TimeMetric.class)
		.setDescription("time metric.")
		.setRequired()
		.build();

	default TimeMetric getTimeMetric() {
		return get(TIME_METRIC);
	}

	default T setTimeMetric(TimeMetric value) {
		return set(TIME_METRIC, value);
	}

	default T setTimeMetric(String value) {
		return set(TIME_METRIC, ParamUtil.searchEnum(TIME_METRIC, value));
	}

	enum TimeMetric {
		SECOND,
		MINUTE,
		HOUR,
		DAY,
		MONTH,
		YEAR
	}

	ParamInfo<Double> WINDOW_LENGTH = ParamInfoFactory
		.createParamInfo("windowLength", Double.class)
		.setDescription("window length.")
		.setRequired()
		.build();

	default Double getWindowLength() {
		return get(WINDOW_LENGTH);
	}

	default T setWindowLength(Double value) {
		return set(WINDOW_LENGTH, value);
	}

	/**
	 * 仅仅在自定义的时候使用。
	 */
	ParamInfo<Integer> SLICE_WINDOW_NUMBER = ParamInfoFactory
		.createParamInfo("sliceWindowNumber", Integer.class)
		.setDescription("slice window number.")
		.setRequired()
		.build();

	default Integer getSliceWindowNumber() {
		return get(SLICE_WINDOW_NUMBER);
	}

	default T setSliceWindowNumber(Integer value) {
		return set(SLICE_WINDOW_NUMBER, value);
	}
}
