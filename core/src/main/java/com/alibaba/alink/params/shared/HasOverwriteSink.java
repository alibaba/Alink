package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOverwriteSink<T> extends WithParams<T> {
	ParamInfo <Boolean> OVERWRITE_SINK = ParamInfoFactory
		.createParamInfo("overwriteSink", Boolean.class)
		.setDescription("Whether to overwrite existing data.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getOverwriteSink() {
		return get(OVERWRITE_SINK);
	}

	default T setOverwriteSink(Boolean value) {
		return set(OVERWRITE_SINK, value);
	}
}
