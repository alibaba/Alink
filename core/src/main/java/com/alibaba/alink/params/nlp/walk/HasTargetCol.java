package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTargetCol<T> extends WithParams <T> {
	ParamInfo <String> TARGET_COL = ParamInfoFactory
		.createParamInfo("targetCol", String.class)
		.setDescription("target col name")
		.setAlias(new String[] {"targetColName", "node1"})
		.setRequired()
		.build();

	default String getTargetCol() {return get(TARGET_COL);}

	default T setTargetCol(String value) {return set(TARGET_COL, value);}
}
