package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface SingleSourceShortestPathParams<T> extends
	CommonGraphParams <T>,
	HasEdgeWeightCol <T>,
	HasMaxIterDefaultAs50 <T> {

	@NameCn("源点的值")
	@DescCn("源点的值")
	ParamInfo <String> SOURCE_POINT = ParamInfoFactory
		.createParamInfo("sourcePoint", String.class)
		.setDescription("source point value")
		.setRequired()
		.build();

	default String getSourcePoint() {return get(SOURCE_POINT);}

	default T setSourcePoint(String value) {return set(SOURCE_POINT, value);}

}
