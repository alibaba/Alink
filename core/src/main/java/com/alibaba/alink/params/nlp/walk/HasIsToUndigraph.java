package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIsToUndigraph<T> extends WithParams <T> {
	/**
	 * @cn-name 是否转无向图
	 * @cn 选为true时，会将当前图转成无向图，然后再游走
	 */
	ParamInfo <Boolean> IS_TO_UNDIGRAPH = ParamInfoFactory
		.createParamInfo("isToUndigraph", Boolean.class)
		.setDescription("is to undigraph")
		.setHasDefaultValue(false)
		.setAlias(new String[] {"isToUnDigraph"})
		.build();

	default Boolean getIsToUndigraph() {return get(IS_TO_UNDIGRAPH);}

	default T setIsToUndigraph(Boolean value) {return set(IS_TO_UNDIGRAPH, value);}
}
