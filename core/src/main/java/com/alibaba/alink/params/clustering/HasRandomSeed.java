package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRandomSeed<T> extends
	WithParams <T> {
	ParamInfo <Integer> RANDOM_SEED = ParamInfoFactory
		.createParamInfo("randomSeed", Integer.class)
		.setDescription("Random seed, it should be positive integer")
		.setHasDefaultValue(0)
		.build();

	default Integer getRandomSeed() {
		return get(RANDOM_SEED);
	}

	default T setRandomSeed(Integer value) {
		return set(RANDOM_SEED, value);
	}

}
