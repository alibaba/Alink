package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface MultiSourceShortestPathParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs50 <T> {

	@NameCn("源点的列")
	@DescCn("源点的列")
	ParamInfo <String> SOURCE_POINT_COL = ParamInfoFactory
		.createParamInfo("sourcePointCol", String.class)
		.setDescription("source point value")
		.setRequired()
		.build();

	default String getSourcePointCol() {return get(SOURCE_POINT_COL);}

	default T setSourcePointCol(String value) {return set(SOURCE_POINT_COL, value);}

}
