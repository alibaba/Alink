package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasAppendType<T> extends WithParams <T> {
	ParamInfo <AppendType> APPEND_TYPE = ParamInfoFactory
		.createParamInfo("appendType", AppendType.class)
		.setDescription("append type. DENSE or UNIQUE")
		.setHasDefaultValue(AppendType.DENSE)
		.setAlias(new String[] {"AppendType"})
		.build();

	default AppendType getAppendType() {
		return get(APPEND_TYPE);
	}

	default T setAppendType(AppendType value) {
		return set(APPEND_TYPE, value);
	}

	default T setAppendType(String value) {
		return set(APPEND_TYPE, ParamUtil.searchEnum(APPEND_TYPE, value));
	}

	enum AppendType {
		DENSE,
		UNIQUE
	}
}
