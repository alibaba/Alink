package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEpsilon<T> extends WithParams <T> {

	@NameCn("邻域距离阈值")
	@DescCn("邻域距离阈值")
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("epsilon")
		.setRequired()
		.build();

	default Double getEpsilon() {return get(EPSILON);}

	default T setEpsilon(Double value) {return set(EPSILON, value);}
}
