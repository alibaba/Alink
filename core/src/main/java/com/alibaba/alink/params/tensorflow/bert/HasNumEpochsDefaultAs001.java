package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumEpochsDefaultAs001<T> extends WithParams <T> {

	ParamInfo <Double> NUM_EPOCHS = ParamInfoFactory
		.createParamInfo("numEpochs", Double.class)
		.setDescription("num epochs")
		.setHasDefaultValue(0.01)
		.build();

	default Double getNumEpochs() {
		return get(NUM_EPOCHS);
	}

	default T setNumEpochs(Double value) {
		return set(NUM_EPOCHS, value);
	}
}
