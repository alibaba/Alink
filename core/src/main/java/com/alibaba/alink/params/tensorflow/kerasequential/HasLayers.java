package com.alibaba.alink.params.tensorflow.kerasequential;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLayers<T> extends WithParams <T> {

	ParamInfo <String[]> LAYERS = ParamInfoFactory
		.createParamInfo("layers", String[].class)
		.setDescription("Description for layers, in Python language, "
			+ "for example \"Conv1D(256, 5, padding='same', activation='relu')\"")
		.setRequired()
		.build();

	default String[] getLayers() {
		return get(LAYERS);
	}

	default T setLayers(String... layers) {
		return set(LAYERS, layers);
	}
}
