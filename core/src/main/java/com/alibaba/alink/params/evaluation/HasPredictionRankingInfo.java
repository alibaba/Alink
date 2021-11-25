package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.recommendation.KObjectUtil;

public interface HasPredictionRankingInfo<T> extends WithParams <T> {
	/**
	 * @cn-name Object列列名
	 * @cn Object列列名
	 */
	ParamInfo <String> PREDICTION_RANKING_INFO = ParamInfoFactory
		.createParamInfo("predictionRankingInfo", String.class)
		.setDescription("the label of ranking in prediction col")
		.setHasDefaultValue(KObjectUtil.OBJECT_NAME)
		.build();

	default String getPredictionRankingInfo() {
		return get(PREDICTION_RANKING_INFO);
	}

	default T setPredictionRankingInfo(String value) {
		return set(PREDICTION_RANKING_INFO, value);
	}
}
