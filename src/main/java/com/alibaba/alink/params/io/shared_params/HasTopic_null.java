package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTopic_null<T> extends WithParams<T> {
	ParamInfo <String> TOPIC = ParamInfoFactory
		.createParamInfo("topic", String.class)
		.setDescription("topic")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"topicName"})
		.build();

	default String getTopic() {
		return get(TOPIC);
	}

	default T setTopic(String value) {
		return set(TOPIC, value);
	}
}
