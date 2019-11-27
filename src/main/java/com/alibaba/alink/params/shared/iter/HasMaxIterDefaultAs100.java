package com.alibaba.alink.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the maximum iterations.
 */
public interface HasMaxIterDefaultAs100<T> extends WithParams<T> {
	ParamInfo <Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("Maximum iterations, The default value is 100")
		.setHasDefaultValue(100)
		.setAlias(new String[] {"maxIteration"})
		.build();

	default Integer getMaxIter() {
		return get(MAX_ITER);
	}

	default T setMaxIter(Integer value) {
		return set(MAX_ITER, value);
	}
}
