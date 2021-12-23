package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPort<T> extends WithParams <T> {
	/**
	 * @cn-name 端口
	 * @cn 端口
	 */
	ParamInfo <String> PORT = ParamInfoFactory
		.createParamInfo("port", String.class)
		.setDescription("port")
		.setRequired()
		.build();

	default String getPort() {return get(PORT);}

	default T setPort(String value) {return set(PORT, value);}
}
