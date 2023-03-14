package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPredictNum<T> extends WithParams <T> {
	ParamInfo <Integer> PREDICT_NUM = ParamInfoFactory
		.createParamInfo("predictNum", Integer.class)
		.setDescription("the predict num")
		.setAlias(new String[] {"forecastStep"})
		.setRequired()
		.build();

	default Integer getPredictNum() {
		return get(PREDICT_NUM);
	}

	default T setPredictNum(Integer value) {
		return set(PREDICT_NUM, value);
	}
}
