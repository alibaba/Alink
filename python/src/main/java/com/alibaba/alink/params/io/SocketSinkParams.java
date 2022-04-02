package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SocketSinkParams<T> extends WithParams <T> {
	ParamInfo <String> HOST = ParamInfoFactory
		.createParamInfo("host", String.class)
		.setDescription("host")
		.setRequired()
		.build();

	default String getHost() {return get(HOST);}

	default T setHost(String value) {return set(HOST, value);}

	ParamInfo <Integer> PORT = ParamInfoFactory
		.createParamInfo("port", Integer.class)
		.setDescription("port")
		.setRequired()
		.build();

	default Integer getPort() {return get(PORT);}

	default T setPort(Integer value) {return set(PORT, value);}
}
