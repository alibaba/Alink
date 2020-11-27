package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the number of thread in one process.
 */
public interface HasNumThreads<T> extends WithParams <T> {

	ParamInfo <Integer> NUM_THREADS = ParamInfoFactory
		.createParamInfo("numThreads", Integer.class)
		.setDescription("Thread number of operator.")
		.setHasDefaultValue(1)
		.build();

	default Integer getNumThreads() {
		return get(NUM_THREADS);
	}

	default T setNumThreads(Integer value) {
		return set(NUM_THREADS, value);
	}
}
