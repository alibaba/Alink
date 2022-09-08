package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface CalcVertexParams<T> extends GraphVertexCols <T> {
	@NameCn("输出点所在列")
	@DescCn("输出点所在列")
	ParamInfo <String> VERTEX_COL = ParamInfoFactory
		.createParamInfo("vertexCol", String.class)
		.setDescription("vertexCol")
		.setRequired()
		.build();

	default String getVertexCol() {
		return get(VERTEX_COL);
	}

	default T setVertexCol(String value) {
		return set(VERTEX_COL, value);
	}

}
