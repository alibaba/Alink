package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasTrainCol<T> extends WithParams <T> {

	ParamInfo <String> TRAIN_COL = ParamInfoFactory
		.createParamInfo("trainCol", String.class)
		.setDescription("train origin col name")
		.setRequired()
		.build();

	default String getTrainCol() {
		return get(TRAIN_COL);
	}

	default T setTrainCol(String value) {
		return set(TRAIN_COL, value);
	}
}

