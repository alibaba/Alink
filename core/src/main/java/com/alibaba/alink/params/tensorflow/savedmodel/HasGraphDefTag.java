package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasGraphDefTag<T> extends WithParams <T> {
	/**
	 * @cn-name graph标签
	 * @cn graph标签
	 */
	ParamInfo <String> GRAPH_DEF_TAG = ParamInfoFactory
		.createParamInfo("graphDefTag", String.class)
		.setDescription("graph def tag")
		.setHasDefaultValue("serve")
		.build();

	default String getGraphDefTag() {
		return get(GRAPH_DEF_TAG);
	}

	default T setGraphDefTag(String value) {
		return set(GRAPH_DEF_TAG, value);
	}
}
