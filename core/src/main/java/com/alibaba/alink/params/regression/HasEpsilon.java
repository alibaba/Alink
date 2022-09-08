package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEpsilon<T> extends WithParams <T> {
	@NameCn("收敛精度")
	@DescCn("收敛精度")
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("epsilon")
		.setHasDefaultValue(1.0e-5)
		.build();

	default Double getEpsilon() {
		return get(EPSILON);
	}

	default Double setEpsilon() {
		return get(EPSILON);
	}

}
