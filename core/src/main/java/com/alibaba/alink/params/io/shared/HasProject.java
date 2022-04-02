package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasProject<T> extends WithParams <T> {

	@NameCn("project")
	@DescCn("project")
	ParamInfo <String> PROJECT = ParamInfoFactory
		.createParamInfo("project", String.class)
		.setDescription("project name")
		.setRequired()
		.build();

	default String getProject() {
		return get(PROJECT);
	}

	default T setProject(String value) {
		return set(PROJECT, value);
	}
}
