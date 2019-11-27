package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxMemoryInMB<T> extends WithParams<T> {
	ParamInfo<Integer> MAX_MEMORY_IN_MB = ParamInfoFactory
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
