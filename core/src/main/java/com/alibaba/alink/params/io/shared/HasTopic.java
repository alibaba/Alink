package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTopic<T> extends WithParams <T> {
	@NameCn("topic名称")
	@DescCn("topic名称")
	ParamInfo <String> TOPIC = ParamInfoFactory
		.createParamInfo("topic", String.class)
		.setDescription("topic")
		.setRequired()
		.setAlias(new String[] {"topicName"})
		.build();

	default String getTopic() {
		return get(TOPIC);
	}

	default T setTopic(String value) {
		return set(TOPIC, value);
	}
}
