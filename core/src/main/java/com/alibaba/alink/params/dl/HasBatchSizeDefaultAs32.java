package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasBatchSizeDefaultAs32<T> extends WithParams <T> {

	ParamInfo <Integer> BATCH_SIZE = ParamInfoFactory
		.createParamInfo("batchSize", Integer.class)
		.setDescription("mini-batch size")
		.setHasDefaultValue(32)
		.build();

	default Integer getBatchSize() {
		return get(BATCH_SIZE);
	}

	default T setBatchSize(Integer value) {
		return set(BATCH_SIZE, value);
	}
}
