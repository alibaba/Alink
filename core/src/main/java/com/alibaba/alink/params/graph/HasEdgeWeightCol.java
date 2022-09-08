package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEdgeWeightCol<T> extends WithParams <T> {
	@NameCn("边权重列")
	@DescCn("表示边权重的列")
	ParamInfo <String> EDGE_WEIGHT_COL = ParamInfoFactory
		.createParamInfo("edgeWeightCol", String.class)
		.setDescription("edge weight wol")
		.setHasDefaultValue(null)
		.build();

	default String getEdgeWeightCol() {return get(EDGE_WEIGHT_COL);}

	default T setEdgeWeightCol(String value) {return set(EDGE_WEIGHT_COL, value);}
}
