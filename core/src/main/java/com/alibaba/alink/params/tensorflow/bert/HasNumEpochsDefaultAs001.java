package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumEpochsDefaultAs001<T> extends WithParams <T> {
	@NameCn("epoch 数")
	@DescCn("epoch 数")
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
