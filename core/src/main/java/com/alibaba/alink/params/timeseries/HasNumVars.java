package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumVars<T> extends WithParams <T> {

	ParamInfo <Integer> NUM_VARS = ParamInfoFactory
		.createParamInfo("numVars", Integer.class)
		.setDescription("num of variables")
		.setHasDefaultValue(1)
		.build();

	default Integer getNumVars() {
		return get(NUM_VARS);
	}

	default T setNumVars(Integer value) {
		return set(NUM_VARS, value);
	}
}
