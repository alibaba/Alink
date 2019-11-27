package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxBins<T> extends WithParams<T> {
	ParamInfo <Integer> MAX_BINS = ParamInfoFactory
		.createParamInfo("maxBins", Integer.class)
		.setDescription("MAX number of bins for continuous feature")
		.setHasDefaultValue(128)
		.setAlias(new String[] {"binNum"})
		.build();

	default Integer getMaxBins() {
		return get(MAX_BINS);
	}

	default T setMaxBins(Integer value) {
		return set(MAX_BINS, value);
	}
}
