package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumRound<T> extends WithParams <T> {

	@NameCn("树的棵树")
	@DescCn("树的棵树")
	ParamInfo <Integer> NUM_ROUND = ParamInfoFactory
		.createParamInfo("numRound", Integer.class)
		.setDescription("The number of rounds for boosting.")
		.setRequired()
		.build();

	default Integer getNumRound() {
		return get(NUM_ROUND);
	}

	default T setNumRound(Integer numRound) {
		return set(NUM_ROUND, numRound);
	}
}
