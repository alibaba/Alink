package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLayer<T> extends WithParams <T> {

	/**
	 * @cn 输出第几层 encoder layer 的结果， -1 表示最后一层，-2 表示倒数第2层，以此类推
	 * @cn-name 输出第几层 encoder layer 的结果
	 */
	ParamInfo <Integer> LAYER = ParamInfoFactory
		.createParamInfo("layer", Integer.class)
		.setDescription("Which encoder layer to use")
		.setHasDefaultValue(-1)
		.build();

	default Integer getLayer() {
		return get(LAYER);
	}

	default T setLayer(Integer value) {
		return set(LAYER, value);
	}
}
