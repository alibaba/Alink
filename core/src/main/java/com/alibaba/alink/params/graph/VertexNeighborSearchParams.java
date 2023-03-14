package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface VertexNeighborSearchParams<T> extends
	AsUndirectedGraph<T>,
	GraphVertexCols<T>
	{
	@NameCn("节点ID列")
	@DescCn("表示节点ID的列名")
	ParamInfo <String> VERTEX_ID_COL = ParamInfoFactory
		.createParamInfo("vertexIdCol", String.class)
		.setDescription("column name for vertex id")
		.setHasDefaultValue("id")
		.setAlias(new String[] {"vertexIdColName"})
		.build();

	@NameCn("深度")
	@DescCn("寻找邻居的深度")
	ParamInfo <Integer> DEPTH = ParamInfoFactory
		.createParamInfo("depth", Integer.class)
		.setDescription("depth to find neighbors")
		.setHasDefaultValue(1)
		.build();

	@NameCn("起始节点集合")
	@DescCn("起始节点集合")
	ParamInfo <String[]> SOURCES = ParamInfoFactory
		.createParamInfo("sources", String[].class)
		.setDescription("the set of source vertices")
		.setRequired()
		.build();

	default String getVertexIdCol() {return get(VERTEX_ID_COL);}

	default T setVertexIdCol(String value) {return set(VERTEX_ID_COL, value);}

	default Integer getDepth() {return get(DEPTH);}

	default T setDepth(Integer value) {return set(DEPTH, value);}

	default String[] getSources() {return get(SOURCES);}

	default T setSources(String... value) {return set(SOURCES, value);}

}
