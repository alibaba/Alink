package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface ModularityCalParams<T> extends
	CommonGraphParams <T>, HasEdgeWeightCol <T> {

	@NameCn("点列")
	@DescCn("输入点表中点信息所在列")
	ParamInfo <String> VERTEX_COL = ParamInfoFactory
		.createParamInfo("vertexCol", String.class)
		.setDescription("vertex column")
		.setRequired()
		.build();

	default String getVertexCol() {
		return get(VERTEX_COL);
	}

	default T setVertexCol(String value) {
		return set(VERTEX_COL, value);
	}

	@NameCn("社群信息列")
	@DescCn("输入点表中点的社群信息所在列")
	ParamInfo <String> VERTEX_COMMUNITY_COL = ParamInfoFactory
		.createParamInfo("vertexCommunityCol", String.class)
		.setDescription("vertex community column")
		.setRequired()
		.build();

	default String getVertexCommunityCol() {
		return get(VERTEX_COMMUNITY_COL);
	}

	default T setVertexCommunityCol(String value) {
		return set(VERTEX_COMMUNITY_COL, value);
	}

}
