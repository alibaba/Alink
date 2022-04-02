package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSeed<T> extends WithParams <T> {
	@NameCn("采样种子")
	@DescCn("采样种子")
	ParamInfo <Long> SEED = ParamInfoFactory
		.createParamInfo("seed", Long.class)
		.setDescription("seed")
		.setHasDefaultValue(0L)
		.build();

	default Long getSeed() {
		return get(SEED);
	}

	default T setSeed(Long value) {
		return set(SEED, value);
	}
}
