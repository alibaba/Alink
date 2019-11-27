package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumSubsetFeatures<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_SUBSET_FEATURES = ParamInfoFactory
		.createParamInfo("numSubsetFeatures", Integer.class)
		.setDescription("The number of features to consider for splits at each tree node.")
		.setHasDefaultValue(Integer.MAX_VALUE)
		.setAlias(new String[] {"baggingFeatureCount"})
		.build();

	default Integer getNumSubsetFeatures() {
		return get(NUM_SUBSET_FEATURES);
	}

	default T setNumSubsetFeatures(Integer value) {
		return set(NUM_SUBSET_FEATURES, value);
	}
}
