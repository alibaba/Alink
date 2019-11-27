package com.alibaba.alink.params.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTrainRatio<T> extends WithParams<T> {
	ParamInfo <Double> TRAIN_RATIO = ParamInfoFactory
		.createParamInfo("trainRatio", Double.class)
		.setDescription("Ratio for training set and the validation set, range in (0, 1].")
		.setHasDefaultValue(0.8)
		.build();

	default Double getTrainRatio() {
		return get(TRAIN_RATIO);
	}

	default T setTrainRatio(Double value) {
		return set(TRAIN_RATIO, value);
	}
}
