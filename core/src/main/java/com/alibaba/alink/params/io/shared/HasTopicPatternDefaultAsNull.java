package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTopicPatternDefaultAsNull<T> extends WithParams <T> {

	@NameCn("topic pattern")
	@DescCn("topic pattern")
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
