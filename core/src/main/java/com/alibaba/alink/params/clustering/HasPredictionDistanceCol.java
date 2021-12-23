package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPredictionDistanceCol<T> extends WithParams<T>{
	/**
	 * @cn-name 预测距离列名
	 * @cn 预测距离列名
	 */
	ParamInfo<String> PREDICTION_DISTANCE_COL = ParamInfoFactory
		.createParamInfo("predictionDistanceCol", String.class)
		.setDescription("Column name of prediction.")
		.build();

	default String getPredictionDistanceCol() {
		return get(PREDICTION_DISTANCE_COL);
	}

	default T setPredictionDistanceCol(String colName) {
		return set(PREDICTION_DISTANCE_COL, colName);
	}
}
