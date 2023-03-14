package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDriverName<T> extends WithParams <T> {
	/**
	 * @cn Driver名称
	 */
	@NameCn("Driver名称")
	@DescCn("Driver的名称")
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
