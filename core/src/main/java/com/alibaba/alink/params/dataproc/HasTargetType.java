package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasTargetType<T> extends WithParams <T> {

	/**
	 * @cn-name 目标类型
	 * @cn 转换为的类型，类型应该为JDBC Type。
	 */
	ParamInfo <TargetType> TARGET_TYPE = ParamInfoFactory
		.createParamInfo("targetType", TargetType.class)
		.setDescription("The target type of numerical column cast function.")
		.setRequired()
		.setAlias(new String[] {"newType"})
		.build();

	default TargetType getTargetType() {
		return get(TARGET_TYPE);
	}

	default T setTargetType(TargetType value) {
		return set(TARGET_TYPE, value);
	}

	default T setTargetType(String value) {
		return set(TARGET_TYPE, ParamUtil.searchEnum(TARGET_TYPE, value));
	}

	enum TargetType {
		STRING,
		VARCHAR,
		FLOAT,
		DOUBLE,
		INT,
		BIGINT,
		LONG,
		BOOLEAN
	}

}
