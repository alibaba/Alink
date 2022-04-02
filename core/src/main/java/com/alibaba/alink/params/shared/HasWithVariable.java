package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.linalg.Vector;

public interface HasWithVariable<T> extends WithParams <T> {
	ParamInfo <String> WITH_VARIABLE = ParamInfoFactory
		.createParamInfo("WithVariable", String.class)
		.setDescription("the variable with the function")
		.build();

	default String getWithVariable() {
		return get(WITH_VARIABLE);
	}

	default T setWithVariable(Double value) {
		return set(WITH_VARIABLE, String.valueOf(value));
	}

	default T setWithVariable(Integer value) {
		return set(WITH_VARIABLE, String.valueOf(value));
	}

	default T setWithVariable(Vector vec) {
		return set(WITH_VARIABLE, vec.toString());
	}

	default T setWithVariable(String value) {
		return set(WITH_VARIABLE, value);
	}

}