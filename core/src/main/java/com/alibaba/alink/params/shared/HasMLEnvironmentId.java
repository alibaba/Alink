package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying the id of MLEnvironment.
 */
public interface HasMLEnvironmentId<T> extends WithParams <T> {

	@NameCn("ML环境ID")
	@DescCn("ML环境ID")
	ParamInfo <Long> ML_ENVIRONMENT_ID = ParamInfoFactory
		.createParamInfo("MLEnvironmentId", Long.class)
		.setDescription("ID of ML environment.")
		.setHasDefaultValue(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
		.build();

	default Long getMLEnvironmentId() {
		return get(ML_ENVIRONMENT_ID);
	}

	default T setMLEnvironmentId(Long value) {
		return set(ML_ENVIRONMENT_ID, value);
	}
}
