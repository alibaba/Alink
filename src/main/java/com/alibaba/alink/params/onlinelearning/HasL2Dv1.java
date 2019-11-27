package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasL2Dv1<T> extends WithParams<T> {
	ParamInfo <Double> L2 = ParamInfoFactory
		.createParamInfo("l2", Double.class)
		.setDescription("l2")
		.setHasDefaultValue(1.0)
		.setAlias(new String[] {"L2"})
		.build();

	default Double getL2() {return get(L2);}

	default T setL2(Double value) {return set(L2, value);}
}
