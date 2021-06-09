package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIsToUndigraph<T> extends WithParams <T> {
	ParamInfo <Boolean> IS_TO_UNDIGRAPH = ParamInfoFactory
		.createParamInfo("isToUndigraph", Boolean.class)
		.setDescription("is to undigraph")
		.setHasDefaultValue(false)
		.setAlias(new String[] {"isToUnDigraph"})
		.build();

	default Boolean getIsToUndigraph() {return get(IS_TO_UNDIGRAPH);}

	default T setIsToUndigraph(Boolean value) {return set(IS_TO_UNDIGRAPH, value);}
}
