package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumEpochsDefaultAs10<T> extends WithParams <T> {

	ParamInfo <Integer> NUM_EPOCHS = ParamInfoFactory
		.createParamInfo("numEpochs", Integer.class)
		.setDescription("num epochs")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"numIter"})
		.build();

	default Integer getNumEpochs() {
		return get(NUM_EPOCHS);
	}

	default T setNumEpochs(Integer value) {
		return set(NUM_EPOCHS, value);
	}
}
