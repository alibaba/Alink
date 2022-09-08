package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface CommonNeighborsTrainParams<T>
	extends GraphVertexCols <T>,
	NeedTransformIDParams <T> {

	@NameCn("是否二部图")
	@DescCn("是否二部图")
	ParamInfo <Boolean> IS_BIPARTITE_GRAPH = ParamInfoFactory
		.createParamInfo("isBipartiteGraph", Boolean.class)
		.setDescription("is bipartite graph or not")
		.setHasDefaultValue(false)
		.build();

	default Boolean getIsBipartiteGraph() {return get(IS_BIPARTITE_GRAPH);}

	default T setIsBipartiteGraph(Boolean value) {return set(IS_BIPARTITE_GRAPH, value);}
}
