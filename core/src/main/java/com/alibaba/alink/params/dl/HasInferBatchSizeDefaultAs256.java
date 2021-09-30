package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasInferBatchSizeDefaultAs256<T> extends WithParams <T> {

	ParamInfo <Integer> INFER_BATCH_SIZE = ParamInfoFactory
		.createParamInfo("inferBatchSize", Integer.class)
		.setDescription("mini-batch size for inference")
		.setHasDefaultValue(256)
		.build();

	default Integer getInferBatchSize() {
		return get(INFER_BATCH_SIZE);
	}

	default T setInferBatchSize(Integer value) {
		return set(INFER_BATCH_SIZE, value);
	}
}
