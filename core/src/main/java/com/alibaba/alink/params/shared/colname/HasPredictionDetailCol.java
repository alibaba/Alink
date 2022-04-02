package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying the column name of prediction detail.
 *
 * <p>The detail is the information of prediction result, such as the probability of each label in classifier.
 */
public interface HasPredictionDetailCol<T> extends WithParams <T> {

	@NameCn("预测详细信息列名")
	@DescCn("预测详细信息列名")
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
