package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasRandomSeed<T> extends
	WithParams <T> {
	@NameCn("随机数种子")
	@DescCn("随机数种子")
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
