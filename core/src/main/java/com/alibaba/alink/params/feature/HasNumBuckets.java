package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Number of buckets.
 */
public interface HasNumBuckets<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_BUCKETS = ParamInfoFactory
		.createParamInfo("numBuckets", Integer.class)
		.setDescription("number of buckets")
		.setHasDefaultValue(2)
		.build();

	default Integer getNumBuckets() {
		return get(NUM_BUCKETS);
	}

	default T setNumBuckets(Integer value) {
		return set(NUM_BUCKETS, value);
	}
}
