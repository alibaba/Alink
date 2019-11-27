package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: The number of features. It will be the length of the output vector.
 */
public interface HasNumFeatures<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_FEATURES = ParamInfoFactory
		.createParamInfo("numFeatures", Integer.class)
		.setDescription("The number of features. It will be the length of the output vector.")
		.setHasDefaultValue(1 << 18)
		.build();

	default Integer getNumFeatures() {
		return get(NUM_FEATURES);
	}

	default T setNumFeatures(Integer value) {
		return set(NUM_FEATURES, value);
	}

}
