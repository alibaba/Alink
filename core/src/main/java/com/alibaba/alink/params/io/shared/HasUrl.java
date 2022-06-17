package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasUrl<T> extends WithParams <T> {

	@NameCn("URL地址")
	@DescCn("URL地址")
	ParamInfo <String> URL = ParamInfoFactory
		.createParamInfo("url", String.class)
		.setDescription("url")
		.setRequired()
		.build();

	default String getUrl() {
		return get(URL);
	}

	default T setUrl(String value) {
		return set(URL, value);
	}
}
