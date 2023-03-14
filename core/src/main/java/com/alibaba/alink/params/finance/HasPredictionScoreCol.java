package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPredictionScoreCol<T> extends WithParams <T> {

	@NameCn("预测分数列")
	@DescCn("预测分数列")
	ParamInfo <String> PREDICTION_SCORE_COL = ParamInfoFactory
		.createParamInfo("predictionScoreCol", String.class)
		.setDescription("prediction score")
		.setRequired()
		.build();

	default String getPredictionScoreCol() {
		return get(PREDICTION_SCORE_COL);
	}

	default T setPredictionScoreCol(String colName) {
		return set(PREDICTION_SCORE_COL, colName);
	}
}
