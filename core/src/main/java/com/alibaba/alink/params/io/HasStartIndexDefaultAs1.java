package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasStartIndexDefaultAs1<T> extends WithParams <T> {
	/**
	 * @cn-name 起始索引
	 * @cn 起始索引
	 */
	ParamInfo <Integer> START_INDEX = ParamInfoFactory
		.createParamInfo("startIndex", Integer.class)
		.setDescription("start index")
		.setHasDefaultValue(1)
		.build();

	default Integer getStartIndex() {
		return get(START_INDEX);
	}

	default T setStartIndex(Integer value) {
		return set(START_INDEX, value);
	}
}
