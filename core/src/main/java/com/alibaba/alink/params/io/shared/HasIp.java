package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIp<T> extends WithParams <T> {
	/**
	 * @cn-name IP地址
	 * @cn IP地址
	 */
	ParamInfo <String> IP = ParamInfoFactory
		.createParamInfo("ip", String.class)
		.setDescription("ip")
		.setRequired()
		.build();

	default String getIp() {return get(IP);}

	default T setIp(String value) {return set(IP, value);}
}
