package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasUsername<T> extends WithParams <T> {
	@NameCn("用户名")
	@DescCn("用户名")
	ParamInfo <String> USERNAME = ParamInfoFactory
		.createParamInfo("username", String.class)
		.setDescription("username")
		.setRequired()
		.setAlias(new String[] {"userName"})
		.build();

	default String getUsername() {
		return get(USERNAME);
	}

	default T setUsername(String value) {
		return set(USERNAME, value);
	}
}
