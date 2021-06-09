package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasWeightCol<T> extends WithParams <T> {
	ParamInfo <String> WEIGHT_COL = ParamInfoFactory
		.createParamInfo("weightCol", String.class)
		.setDescription("weight col name")
		.setAlias(new String[] {"weightColName", "value"})
		.setHasDefaultValue(null)
		.build();

	default String getWeightCol() {return get(WEIGHT_COL);}

	default T setWeightCol(String value) {return set(WEIGHT_COL, value);}
}
