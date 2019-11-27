package com.alibaba.alink.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumIterDefaultAs5<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_ITER = ParamInfoFactory
		.createParamInfo("numIter", Integer.class)
		.setDescription("Number of iterations, The default value is 1")
		.setHasDefaultValue(5)
		.setAlias(new String[] {"maxIter", "iter", "numIteration"})
		.build();

	default Integer getNumIter() {
		return get(NUM_ITER);
	}

	default T setNumIter(Integer value) {
		return set(NUM_ITER, value);
	}
}
