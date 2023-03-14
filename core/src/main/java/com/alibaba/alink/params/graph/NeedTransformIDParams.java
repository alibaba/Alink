package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface NeedTransformIDParams<T> extends WithParams <T> {
	@NameCn("是否需要转换到ID")
	@DescCn("是否需要转换到ID")
	ParamInfo <Boolean> TRANSFORM_ID = ParamInfoFactory
		.createParamInfo("needTransformID", Boolean.class)
		.setDescription("transform values to id or not")
		.setAlias(new String[]{"transformID"})
		.setHasDefaultValue(true)
		.build();

	default Boolean getNeedTransformID() {return get(TRANSFORM_ID);}

	default T setNeedTransformID(Boolean value) {return set(TRANSFORM_ID, value);}
}
