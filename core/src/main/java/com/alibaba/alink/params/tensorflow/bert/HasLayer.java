package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLayer<T> extends WithParams <T> {


	ParamInfo <Integer> LAYER = ParamInfoFactory
		.createParamInfo("layer", Integer.class)
		.setDescription("layer")
		.setHasDefaultValue(-1)
		.build();

	default Integer getLayer() {
		return get(LAYER);
	}

	default T setLayer(Integer value) {
		return set(LAYER, value);
	}
}
