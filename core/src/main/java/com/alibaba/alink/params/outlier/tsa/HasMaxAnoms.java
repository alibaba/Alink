package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxAnoms<T> extends WithParams <T> {

	ParamInfo <Integer> MAX_ANOMS = ParamInfoFactory
		.createParamInfo("maxAnoms", Integer.class)
		.setDescription("the max number of anomalier that" +
			" algo will detect as a percentage pf the data.")
		.setHasDefaultValue(10)
		.build();

	default Integer getMaxAnoms() {
		return get(MAX_ANOMS);
	}

	default T setMaxAnoms(Integer value) {
		return set(MAX_ANOMS, value);
	}
}
