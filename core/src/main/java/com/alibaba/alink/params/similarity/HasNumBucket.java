package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;


public interface HasNumBucket<T> extends WithParams <T> {
	ParamInfo <Integer> NUM_BUCKET = ParamInfoFactory
		.createParamInfo("numBucket", Integer.class)
		.setDescription("the number of bucket")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"bucket"})
		.build();

	default Integer getNumBucket() {
		return get(NUM_BUCKET);
	}

	default T setNumBucket(Integer value) {
		return set(NUM_BUCKET, value);
	}
}
