package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPredictionClusterCol<T> extends WithParams <T> {
	@NameCn("预测距离列名")
	@DescCn("预测距离列名")
	ParamInfo <String> PREDICTION_CLUSTER_COL = ParamInfoFactory
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
