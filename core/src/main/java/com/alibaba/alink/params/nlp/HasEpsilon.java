package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.nlp.TextRankConst;

public interface HasEpsilon<T> extends WithParams <T> {
	/**
	 * @cn-name 收敛阈值
	 * @cn 收敛阈值
	 */
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("converge threshold")
		.setHasDefaultValue(TextRankConst.EPSILON)
		.build();

	default Double getEpsilon() {
		return get(EPSILON);
	}

	default T setEpsilon(Double value) {
		return set(EPSILON, value);
	}
}
