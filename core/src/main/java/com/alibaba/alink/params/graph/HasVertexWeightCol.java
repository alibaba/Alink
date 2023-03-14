package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasVertexWeightCol<T> extends WithParams <T> {

	@NameCn("点的权重所在列")
	@DescCn("点的权重所在列，如果不输入就自动补为1。")
	ParamInfo <String> VERTEX_WEIGHT_COL = ParamInfoFactory
		.createParamInfo("vertexWeightCol", String.class)
		.setDescription("vertex Weight Col")
		.setHasDefaultValue(null)
		.build();

	default String getVertexWeightCol() {return get(VERTEX_WEIGHT_COL);}

	default T setVertexWeightCol(String value) {return set(VERTEX_WEIGHT_COL, value);}
}
