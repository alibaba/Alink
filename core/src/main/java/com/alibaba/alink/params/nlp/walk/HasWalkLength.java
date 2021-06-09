package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasWalkLength<T> extends WithParams <T> {
	ParamInfo <Integer> WALK_LENGTH = ParamInfoFactory
		.createParamInfo("walkLength", Integer.class)
		.setDescription("walk length")
		.setRequired()
		.build();

	default Integer getWalkLength() {return get(WALK_LENGTH);}

	default T setWalkLength(Integer value) {return set(WALK_LENGTH, value);}
}
