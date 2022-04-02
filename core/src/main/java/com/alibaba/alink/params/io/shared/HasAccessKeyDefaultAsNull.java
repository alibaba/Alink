package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAccessKeyDefaultAsNull<T> extends WithParams <T> {
	@NameCn("accessKey")
	@DescCn("accessKey")
	ParamInfo <String> ACCESS_KEY = ParamInfoFactory
		.createParamInfo("accessKey", String.class)
		.setDescription("access key")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"accesskey"})
		.build();

	default String getAccessKey() {
		return get(ACCESS_KEY);
	}

	default T setAccessKey(String value) {
		return set(ACCESS_KEY, value);
	}
}
