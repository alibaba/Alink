package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumBucketsArray<T> extends WithParams <T> {
	@NameCn("quantile个数")
	@DescCn("quantile个数，每一列对应数组中一个元素。")
	ParamInfo <Integer[]> NUM_BUCKETS_ARRAY = ParamInfoFactory
		.createParamInfo("numBucketsArray", Integer[].class)
		.setDescription("Array of num bucket")
		.setHasDefaultValue(null)
		.build();

	default Integer[] getNumBucketsArray() {
		return get(NUM_BUCKETS_ARRAY);
	}

	default T setNumBucketsArray(Integer... value) {
		return set(NUM_BUCKETS_ARRAY, value);
	}
}
