package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasBaseScore<T> extends WithParams <T> {

	@NameCn("Base score")
	@DescCn("Base score")
	ParamInfo <Double> BASE_SCORE = ParamInfoFactory
		.createParamInfo("baseScore", Double.class)
		.setDescription("The initial prediction score of all instances, global bias.")
		.setHasDefaultValue(0.5)
		.build();

	default Double getBaseScore() {
		return get(BASE_SCORE);
	}

	default T setBaseScore(Double baseScore) {
		return set(BASE_SCORE, baseScore);
	}
}
