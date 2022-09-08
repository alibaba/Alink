package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPredictBatchSize<T> extends WithParams <T> {

	@NameCn("Predict BatchSize")
	@DescCn("Predict BatchSize")
	ParamInfo <Integer> PREDICT_BATCH_SIZE = ParamInfoFactory
		.createParamInfo("predictBatchSize", Integer.class)
		.setDescription("batch size for mapper.")
		.setHasDefaultValue(null)
		.build();

	default Integer getPredictBatchSize() {
		return get(PREDICT_BATCH_SIZE);
	}

	default T setPredictBatchSize(Integer batchSize) {
		return set(PREDICT_BATCH_SIZE, batchSize);
	}

}
