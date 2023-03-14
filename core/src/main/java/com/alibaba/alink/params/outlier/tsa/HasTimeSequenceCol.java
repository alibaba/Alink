package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTimeSequenceCol<T> extends WithParams <T> {

	ParamInfo <String> TIME_SEQUENCE_COL = ParamInfoFactory
		.createParamInfo("timeSequenceCol", String.class)
		.setDescription("the time sequence col")
		.setRequired()
		.build();

	default String getTimeSequenceCol() {
		return get(TIME_SEQUENCE_COL);
	}

	default T setTimeSequenceCol(String value) {
		return set(TIME_SEQUENCE_COL, value);
	}
}
