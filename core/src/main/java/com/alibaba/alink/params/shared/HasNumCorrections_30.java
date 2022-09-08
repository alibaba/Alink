package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;


public interface HasNumCorrections_30<T> extends WithParams <T> {
	ParamInfo <Integer> NUM_CORRECTIONS = ParamInfoFactory
		.createParamInfo("numCorrections", Integer.class)
		.setDescription("num corrections, only used by LBFGS and owlqn.")
		.setHasDefaultValue(30)
		.build();

	default int getNumCorrections() {
		return get(NUM_CORRECTIONS);
	}

	default T setNumCorrections(int value) {
		return set(NUM_CORRECTIONS, value);
	}
}
