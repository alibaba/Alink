package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the size of check.
 */
public interface HasSize<T> extends WithParams <T> {
	/**
	 * @cn-name 向量大小
	 * @cn 用于判断向量的大小是否和设置的一致
	 */
	ParamInfo <Integer> SIZE = ParamInfoFactory
		.createParamInfo("size", Integer.class)
		.setDescription("size of some thing.")
		.setRequired()
		.build();

	default Integer getSize() {
		return get(SIZE);
	}

	default T setSize(Integer value) {
		return set(SIZE, value);
	}
}
