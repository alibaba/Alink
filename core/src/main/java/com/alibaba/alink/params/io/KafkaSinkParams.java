package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasDataFormat;
import com.alibaba.alink.params.io.shared.HasTopic;

public interface KafkaSinkParams<T> extends WithParams <T>,
	HasTopic <T>, HasDataFormat <T>, HasFieldDelimiterDefaultAsComma <T>, HasProperties <T> {

	@NameCn("bootstrapServers")
	@DescCn("bootstrapServers")
	ParamInfo <String> BOOTSTRAP_SERVERS = ParamInfoFactory
		.createParamInfo("bootstrapServers", String.class)
		.setDescription("kafka bootstrap servers")
		.setRequired()
		.setAlias(new String[] {"bootstrap.servers", "boostrapServers"})
		.build();

	default String getBootstrapServers() {
		return get(BOOTSTRAP_SERVERS);
	}

	default T setBootstrapServers(String value) {
		return set(BOOTSTRAP_SERVERS, value);
	}
}
