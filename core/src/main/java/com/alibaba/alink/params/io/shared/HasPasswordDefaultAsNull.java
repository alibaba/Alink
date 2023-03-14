package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPasswordDefaultAsNull<T> extends WithParams <T> {

	@NameCn("密码")
	@DescCn("密码")
	ParamInfo <String> PASSWORD = ParamInfoFactory
		.createParamInfo("password", String.class)
		.setDescription("password")
		.setHasDefaultValue(null)
		.build();

	default String getPassword() {
		return get(PASSWORD);
	}

	default T setPassword(String value) {
		return set(PASSWORD, value);
	}
}
