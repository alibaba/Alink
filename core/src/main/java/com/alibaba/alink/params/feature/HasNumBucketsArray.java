package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;


public interface HasNumBucketsArray<T> extends WithParams<T> {
	ParamInfo <Integer[]> NUM_BUCKETS_ARRAY = ParamInfoFactory
		.createParamInfo("numBucketsArray", Integer[].class)
		.setDescription("Array of num bucket")
		.setHasDefaultValue(null)
		.build();

	default Integer[] getNumBucketsArray() {
		return get(NUM_BUCKETS_ARRAY);
	}

	default T setNumBucketsArray(Integer[] value) {
		return set(NUM_BUCKETS_ARRAY, value);
	}
}
