package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface GraphVertexCols<T> extends WithParams <T> {
	@NameCn("边表中起点所在列")
	@DescCn("边表中起点所在列")
	ParamInfo <String> EDGE_SOURCE_COL = ParamInfoFactory
		.createParamInfo("edgeSourceCol", String.class)
		.setDescription("source column of edge")
		.setRequired()
		.build();

	default String getEdgeSourceCol() {
		return get(EDGE_SOURCE_COL);
	}

	default T setEdgeSourceCol(String value) {
		return set(EDGE_SOURCE_COL, value);
	}

	@NameCn("边表中终点所在列")
	@DescCn("边表中终点所在列")
	ParamInfo <String> EDGE_TARGET_COL = ParamInfoFactory
		.createParamInfo("edgeTargetCol", String.class)
		.setDescription("target column of edge")
		.setRequired()
		.build();

	default String getEdgeTargetCol() {
		return get(EDGE_TARGET_COL);
	}

	default T setEdgeTargetCol(String value) {
		return set(EDGE_TARGET_COL, value);
	}

}
