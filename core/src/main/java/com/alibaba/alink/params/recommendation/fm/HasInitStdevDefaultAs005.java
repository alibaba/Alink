package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * initial stdev.
 */
public interface HasInitStdevDefaultAs005<T> extends WithParams <T> {
	ParamInfo <Double> INIT_STDEV = ParamInfoFactory
			.createParamInfo("initStdev", Double.class)
			.setDescription("init stdev")
			.setHasDefaultValue(0.05)
			.build();

	default Double getInitStdev() {
		return get(INIT_STDEV);
	}

	default T setInitStdev(Double value) {
		return set(INIT_STDEV, value);
	}
}
