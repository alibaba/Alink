package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAccessId<T> extends WithParams <T> {

	ParamInfo <String> ACCESS_ID = ParamInfoFactory
		.createParamInfo("accessId", String.class)
		.setDescription("access id")
		.setRequired()
		.build();

	default String getAccessId() {
		return get(ACCESS_ID);
	}

	default T setAccessId(String value) {
		return set(ACCESS_ID, value);
	}
}
