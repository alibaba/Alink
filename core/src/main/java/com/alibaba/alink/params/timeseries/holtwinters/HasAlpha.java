package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.ParamValidator;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.RangeValidator;

public interface HasAlpha<T> extends WithParams <T> {

	ParamValidator <Double> validator = new RangeValidator <>(0.0, 1.0)
		.setLeftInclusive(true)
		.setRightInclusive(true);

	/**
	 * @cn-name alpha
	 * @cn alpha
	 */
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("The alpha.")
		.setHasDefaultValue(0.3)
		.setValidator(validator)
		.build();

	default Double getAlpha() {
		return get(ALPHA);
	}

	default T setAlpha(Double value) {
		return set(ALPHA, value);
	}
}
