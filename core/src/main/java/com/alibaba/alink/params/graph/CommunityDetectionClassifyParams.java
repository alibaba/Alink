package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface CommunityDetectionClassifyParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs50 <T>,
	HasVertexCol <T> {

	@NameCn("输入点表中标签所在列")
	@DescCn("输入点表中标签所在列")
	ParamInfo <String> VERTEX_LABEL_COL = ParamInfoFactory
		.createParamInfo("vertexLabelCol", String.class)
		.setDescription("vertex Label Col")
		.setRequired()
		.build();

	default String getVertexLabelCol() {return get(VERTEX_LABEL_COL);}

	default T setVertexLabelCol(String value) {return set(VERTEX_LABEL_COL, value);}

	@NameCn("delta")
	@DescCn("delta参数")
	ParamInfo <Double> DELTA = ParamInfoFactory
		.createParamInfo("delta", Double.class)
		.setDescription("delta param")
		.setHasDefaultValue(0.2)
		.build();

	default Double getDelta() {return get(DELTA);}

	default T setDelta(Double value) {return set(DELTA, value);}

	@NameCn("K值")
	@DescCn("每轮迭代中，设置1/k的node不更新它们的值。这样的设定可能使得社区发现的效果更好。")
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("each iteration, set 1/k of nodes don't update their values.")
		.setHasDefaultValue(40)
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
