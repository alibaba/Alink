package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;

/**
 * Params for multi classification evaluation.
 */
public interface EvalMultiClassParams<T> extends
	HasLabelCol <T>,
	HasPredictionDetailCol <T> {
	/**
	 * @cn-name 预测结果列名
	 * @cn 预测结果列名
	 */
	ParamInfo <String> PREDICTION_COL = ParamInfoFactory
		.createParamInfo("predictionCol", String.class)
		.setDescription("Column name of prediction.")
		.setAlias(new String[] {"predResultColName", "predColName", "outputColName", "predResultCol"})
		.build();

	default String getPredictionCol() {
		return get(PREDICTION_COL);
	}

	default T setPredictionCol(String colName) {
		return set(PREDICTION_COL, colName);
	}
}
