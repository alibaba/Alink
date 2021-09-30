package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxOrder<T> extends WithParams <T> {


	ParamInfo <Integer> MAX_ORDER = ParamInfoFactory
		.createParamInfo("maxOrder", Integer.class)
		.setDescription("max order of p, q")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"upperBound"})
		.build();

	default Integer getMaxOrder() {
		return get(MAX_ORDER);
	}

	default T setMaxOrder(Integer value) {
		return set(MAX_ORDER, value);
	}
}
