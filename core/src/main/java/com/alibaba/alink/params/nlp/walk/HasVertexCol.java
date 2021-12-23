package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasVertexCol<T> extends WithParams <T> {
	/**
	 * @cn-name 节点列名
	 * @cn 用来指定节点列
	 */
	ParamInfo <String> VERTEX_COL = ParamInfoFactory
		.createParamInfo("vertexCol", String.class)
		.setAlias(new String[] {"vertexColName"})
		.setDescription("vertex col name")
		.setRequired()
		.build();

	default String getVertexCol() {return get(VERTEX_COL);}

	default T setVertexCol(String value) {return set(VERTEX_COL, value);}
}
