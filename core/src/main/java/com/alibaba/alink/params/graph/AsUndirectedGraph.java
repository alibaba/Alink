package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface AsUndirectedGraph<T> extends WithParams <T> {
	@NameCn("是否为无向图")
	@DescCn("是否为无向图")
	ParamInfo <Boolean> AS_UNDIRECTED_GRAPH = ParamInfoFactory
		.createParamInfo("isUndirectedGraph", Boolean.class)
		.setDescription("whether the graph is undirected")
		.setHasDefaultValue(true)
		.build();

	default Boolean getAsUndirectedGraph() {
		return get(AS_UNDIRECTED_GRAPH);
	}

	default T setAsUndirectedGraph(Boolean value) {
		return set(AS_UNDIRECTED_GRAPH, value);
	}
}
