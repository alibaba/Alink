package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRandomSeed<T> extends WithParams<T> {
	ParamInfo <Long> RANDOM_SEED = ParamInfoFactory
		.createParamInfo("randomSeed", Long.class)
		.setDescription("Random seed, it should be positive integer")
		.setHasDefaultValue(772209414L)
		.setAlias(new String[] {"seed"})
		.build();

	default Long getRandomSeed() {
		return get(RANDOM_SEED);
	}

	default T setRandomSeed(Long value) {
		return set(RANDOM_SEED, value);
	}
}
