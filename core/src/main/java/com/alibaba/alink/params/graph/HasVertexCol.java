package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasVertexCol<T> extends WithParams <T> {

	@NameCn("输入点表中点所在列")
	@DescCn("输入点表中点所在列")
	ParamInfo <String> VERTEX_COL = ParamInfoFactory
		.createParamInfo("vertexCol", String.class)
		.setDescription("vertex Col")
		.setRequired()
		.build();

	default String getVertexCol() {return get(VERTEX_COL);}

	default T setVertexCol(String value) {return set(VERTEX_COL, value);}
}
