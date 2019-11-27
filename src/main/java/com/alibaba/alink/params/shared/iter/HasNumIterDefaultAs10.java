package com.alibaba.alink.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumIterDefaultAs10<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_ITER = ParamInfoFactory
		.createParamInfo("numIter", Integer.class)
		.setDescription("Number of iterations, The default value is 10")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"numIters", "maxIter", "maxIters"})
		.build();

	default Integer getNumIter() {
		return get(NUM_ITER);
	}

	default T setNumIter(Integer value) {
		return set(NUM_ITER, value);
	}
}
