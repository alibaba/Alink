package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.HasTimeIntervalDv3;

/**
 * Params for binary classification evaluation.
 */
public interface EvalBinaryClassStreamParams<T> extends
	EvalBinaryClassParams <T>,
	HasTimeIntervalDv3 <T> {
	ParamInfo <String> PREDICTION_COL = ParamInfoFactory
		.createParamInfo("predictionCol", String.class)
		.setDescription("Column name of prediction.")
		.setAlias(new String[] {"predResultColName", "predColName", "outputColName", "predResultCol"})
		.setRequired()
		.build();

	default String getPredictionCol() {
		return get(PREDICTION_COL);
	}

	/**
	 * @deprecated predictionCol is not used any more, please set predictionDetailCol.
	 */
	@Deprecated
	default T setPredictionCol(String colName) {
		return set(PREDICTION_COL, colName);
	}
}
