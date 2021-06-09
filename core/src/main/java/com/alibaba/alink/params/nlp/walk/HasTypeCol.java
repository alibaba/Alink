package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTypeCol<T> extends WithParams <T> {
	ParamInfo <String> TYPE_COL = ParamInfoFactory
		.createParamInfo("typeCol", String.class)
		.setAlias(new String[] {"typeColName"})
		.setDescription("type col name")
		.setRequired()
		.build();

	default String getTypeCol() {return get(TYPE_COL);}

	default T setTypeCol(String value) {return set(TYPE_COL, value);}
}
