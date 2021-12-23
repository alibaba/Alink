package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxPatternLengthDefaultAs10<T> extends WithParams <T> {
	/**
	 * @cn-name 最大频繁项集长度
	 * @cn 最大频繁项集长度
	 */
	ParamInfo <Integer> MAX_PATTERN_LENGTH = ParamInfoFactory
		.createParamInfo("maxPatternLength", Integer.class)
		.setDescription("Maximum frequent pattern length")
		.setHasDefaultValue(10)
		.build();

	default Integer getMaxPatternLength() {
		return get(MAX_PATTERN_LENGTH);
	}

	default T setMaxPatternLength(Integer value) {
		return set(MAX_PATTERN_LENGTH, value);
	}
}
