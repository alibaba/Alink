package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface NegativeItemSamplingParams<T> extends
	WithParams <T> {

	@NameCn("采样因子")
	@DescCn("采样因子")
	ParamInfo <Integer> SAMPLING_FACTOR = ParamInfoFactory
		.createParamInfo("samplingFactor", Integer.class)
		.setDescription("")
		.setHasDefaultValue(3)
		.build();

	default Integer getSamplingFactor() {
		return get(SAMPLING_FACTOR);
	}

	default T setSamplingFactor(Integer value) {
		return set(SAMPLING_FACTOR, value);
	}
}
