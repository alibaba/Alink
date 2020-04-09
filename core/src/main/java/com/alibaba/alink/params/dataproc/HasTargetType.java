package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.ParamUtil;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

import java.io.Serializable;

public interface HasTargetType<T> extends WithParams<T> {

	ParamInfo<TargetType> TARGET_TYPE = ParamInfoFactory
		.createParamInfo("targetType", TargetType.class)
		.setDescription("The target type of numerical column cast function.")
		.setRequired()
		.setAlias(new String[]{"newType"})
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

	enum TargetType implements Serializable {
		DOUBLE, INT, BIGINT
	}
}
