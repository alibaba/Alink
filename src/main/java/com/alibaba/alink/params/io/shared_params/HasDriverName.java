package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDriverName<T> extends WithParams<T> {
	ParamInfo <String> DRIVER_NAME = ParamInfoFactory
		.createParamInfo("driverName", String.class)
		.setDescription("driver name")
		.setRequired()
		.build();

	default String getDriverName() {
		return get(DRIVER_NAME);
	}

	default T setDriverName(String value) {
		return set(DRIVER_NAME, value);
	}
}
