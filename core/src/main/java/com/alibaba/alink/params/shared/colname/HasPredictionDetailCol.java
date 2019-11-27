package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the column name of prediction detail.
 *
 * <p>The detail is the information of prediction result, such as the probability of each label in classifier.
 */
public interface HasPredictionDetailCol<T> extends WithParams<T> {

	ParamInfo <String> PREDICTION_DETAIL_COL = ParamInfoFactory
		.createParamInfo("predictionDetailCol", String.class)
		.setDescription("Column name of prediction result, it will include detailed info.")
		.setAlias(new String[] {"predDetailColName"})
		.build();

	default String getPredictionDetailCol() {
		return get(PREDICTION_DETAIL_COL);
	}

	default T setPredictionDetailCol(String colName) {
		return set(PREDICTION_DETAIL_COL, colName);
	}
}
