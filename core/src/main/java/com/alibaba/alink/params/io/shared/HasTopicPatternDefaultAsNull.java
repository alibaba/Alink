package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTopicPatternDefaultAsNull<T> extends WithParams <T> {

	/**
	 * @cn-name "topic pattern"
	 * @cn "topic pattern"
	 */
	ParamInfo <String> TOPIC_PATTERN = ParamInfoFactory
		.createParamInfo("topicPattern", String.class)
		.setDescription("topic pattern")
		.setHasDefaultValue(null)
		.build();

	default String getTopicPattern() {
		return get(TOPIC_PATTERN);
	}

	default T setTopicPattern(String value) {
		return set(TOPIC_PATTERN, value);
	}
}
