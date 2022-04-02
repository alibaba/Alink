package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumBucket<T> extends WithParams <T> {
	@NameCn("分桶个数")
	@DescCn("分桶个数")
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
