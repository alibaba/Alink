package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

/**
 * Params for binary classification evaluation.
 */
public interface EvalBinaryClassParams<T> extends
	HasLabelCol <T>,
	HasPositiveLabelValueString <T> {
	/**
	 * @cn-name 预测详细信息列名
	 * @cn 预测详细信息列名
	 */
	ParamInfo <String> PREDICTION_DETAIL_COL = ParamInfoFactory
		.createParamInfo("predictionDetailCol", String.class)
		.setDescription("Column name of prediction result, it will include detailed info.")
		.setAlias(new String[] {"predDetailColName"})
		.setRequired()
		.build();

	default String getPredictionDetailCol() {
		return get(PREDICTION_DETAIL_COL);
	}

	default T setPredictionDetailCol(String colName) {
		return set(PREDICTION_DETAIL_COL, colName);
	}
}
