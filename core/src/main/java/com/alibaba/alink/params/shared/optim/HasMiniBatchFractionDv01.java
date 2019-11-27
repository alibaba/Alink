package com.alibaba.alink.params.shared.optim;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * fraction of each mini batch to use for update.
 *
 */
public interface HasMiniBatchFractionDv01<T> extends WithParams<T> {

	ParamInfo <Double> MINI_BATCH_FRACTION = ParamInfoFactory
		.createParamInfo("miniBatchFraction", Double.class)
		.setDescription("fraction of each mini batch to use for update")
		.setHasDefaultValue(0.1)
		.build();

	default Double getMiniBatchFraction() {
		return get(MINI_BATCH_FRACTION);
	}

	default T setMiniBatchFraction(Double value) {
		return set(MINI_BATCH_FRACTION, value);
	}
}
