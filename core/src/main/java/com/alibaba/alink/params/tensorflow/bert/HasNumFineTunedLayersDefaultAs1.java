package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumFineTunedLayersDefaultAs1<T> extends WithParams <T> {
	/**
	 * @cn 微调层数
	 * @cn-name 微调层数
	 */
	ParamInfo <Integer> NUM_FINE_TUNED_LAYERS = ParamInfoFactory
		.createParamInfo("numFineTunedLayers", Integer.class)
		.setDescription("number of fine-tuned layers, counting from last one")
		.setAlias(new String[] {"numFinetunedLayers"})
		.setHasDefaultValue(1)
		.build();

	default Integer getNumFineTunedLayers() {
		return get(NUM_FINE_TUNED_LAYERS);
	}

	default T setNumFineTunedLayers(Integer value) {
		return set(NUM_FINE_TUNED_LAYERS, value);
	}
}
