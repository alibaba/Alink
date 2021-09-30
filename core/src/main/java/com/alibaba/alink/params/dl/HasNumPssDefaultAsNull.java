package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumPssDefaultAsNull<T> extends WithParams <T> {


	ParamInfo <Integer> NUM_PSS = ParamInfoFactory
		.createParamInfo("numPSs", Integer.class)
		.setDescription("number of parameter servers")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumPSs() {
		return get(NUM_PSS);
	}

	default T setNumPSs(Integer value) {
		return set(NUM_PSS, value);
	}
}
