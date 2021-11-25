package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPredictionClusterCol<T> extends WithParams<T>{
	/**
	 * @cn-name 预测距离列名
	 * @cn 预测距离列名
	 */
	ParamInfo<String> PREDICTION_CLUSTER_COL = ParamInfoFactory
		.createParamInfo("predictionClusterCol", String.class)
		.setDescription("Column name of prediction.")
		.build();

	default String getPredictionClusterCol() {
		return get(PREDICTION_CLUSTER_COL);
	}

	default T setPredictionClusterCol(String colName) {
		return set(PREDICTION_CLUSTER_COL, colName);
	}
}
