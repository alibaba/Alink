package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPredScoreCol<T> extends WithParams <T> {
	@NameCn("预测得分列")
	@DescCn("用来判断异常的值")
	ParamInfo <String> PRED_SCORE_COL = ParamInfoFactory
		.createParamInfo("predScoreCol", String.class)
		.setDescription("Column name of predicted result.")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"predScoreName", "predScoreColName", "predictionScoreCol"})
		.build();

	default String getPredScoreCol() {
		return get(PRED_SCORE_COL);
	}

	default T setPredScoreCol(String value) {
		return set(PRED_SCORE_COL, value);
	}
}
