package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasL1Dv1<T> extends WithParams<T> {
	ParamInfo <Double> L1 = ParamInfoFactory
		.createParamInfo("l1", Double.class)
		.setDescription("l1")
		.setHasDefaultValue(1.0)
		.setAlias(new String[] {"L1"})
		.build();

	default Double getL1() {return get(L1);}

	default T setL1(Double value) {return set(L1, value);}
}
