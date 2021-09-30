package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTextPairCol<T> extends WithParams <T> {

	ParamInfo <String> TEXT_PAIR_COL = ParamInfoFactory
		.createParamInfo("textPairCol", String.class)
		.setDescription("Name of the text pair column")
		.setAlias(new String[] {"rightSentenceCol"})
		.setHasDefaultValue(null)
		.build();

	default String getTextPairCol() {
		return get(TEXT_PAIR_COL);
	}

	default T setTextPairCol(String colName) {
		return set(TEXT_PAIR_COL, colName);
	}
}
