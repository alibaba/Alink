package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface NeedTransformIDParams<T> extends WithParams <T> {
	ParamInfo <Boolean> TRANSFORM_ID = ParamInfoFactory
		.createParamInfo("needTransformID", Boolean.class)
		.setDescription("transform values to id or not")
		.setAlias(new String[]{"transformID"})
		.setHasDefaultValue(true)
		.build();

	default Boolean getNeedTransformID() {return get(TRANSFORM_ID);}

	default T setNeedTransformID(Boolean value) {return set(TRANSFORM_ID, value);}
}
