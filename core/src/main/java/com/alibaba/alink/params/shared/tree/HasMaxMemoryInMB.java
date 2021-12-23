package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxMemoryInMB<T> extends WithParams <T> {
	/**
	 * @cn-name 树模型中用来加和统计量的最大内存使用数
	 * @cn 树模型中用来加和统计量的最大内存使用数
	 */
	ParamInfo <Integer> MAX_MEMORY_IN_MB = ParamInfoFactory
		.createParamInfo("maxMemoryInMB", Integer.class)
		.setDescription("max memory usage in tree histogram aggregate.")
		.setHasDefaultValue(64)
		.build();

	default Integer getMaxMemoryInMB() {
		return get(MAX_MEMORY_IN_MB);
	}

	default T setMaxMemoryInMB(Integer value) {
		return set(MAX_MEMORY_IN_MB, value);
	}
}
