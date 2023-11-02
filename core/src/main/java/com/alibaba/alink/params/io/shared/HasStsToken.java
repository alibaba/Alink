package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasStsToken<T> extends WithParams <T> {
	@NameCn("sts token")
	@DescCn("sts token")
	ParamInfo <String> STS_TOKEN = ParamInfoFactory
		.createParamInfo("stsToken", String.class)
		.setDescription("sts token")
		.setOptional()
		.setHasDefaultValue("")
		.build();

	default String getStsToken() {
		return get(STS_TOKEN);
	}

	default T setStsToken(String value) {
		return set(STS_TOKEN, value);
	}
}
