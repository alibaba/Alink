package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the column name of the prediction.
 */
public interface HasPredictionCol<T> extends WithParams<T> {

	ParamInfo <String> PREDICTION_COL = ParamInfoFactory
		.createParamInfo("predictionCol", String.class)
		.setDescription("Column name of prediction.")
		.setAlias(new String[] {"predResultColName", "predColName", "outputColName"})
		.setRequired()
		.build();

	default String getPredictionCol() {
		return get(PREDICTION_COL);
	}

	default T setPredictionCol(String colName) {
		return set(PREDICTION_COL, colName);
	}
}
