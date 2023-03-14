package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPredictCol<T> extends WithParams <T> {

	ParamInfo <String> PREDICT_COL = ParamInfoFactory
		.createParamInfo("predictCol", String.class)
		.setDescription("predictCol")
		.setRequired()
		.build();

	default String getPredictCol() {
		return get(PREDICT_COL);
	}

	default T setPredictCol(String value) {
		return set(PREDICT_COL, value);
	}
}
