package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTextCol<T> extends WithParams <T> {

	ParamInfo <String> TEXT_COL = ParamInfoFactory
		.createParamInfo("textCol", String.class)
		.setDescription("Name of the text column")
		.setAlias(new String[] {"selectedCol", "sentenceCol", "leftSentenceCol"})
		.setRequired()
		.build();

	default String getTextCol() {
		return get(TEXT_COL);
	}

	default T setTextCol(String colName) {
		return set(TEXT_COL, colName);
	}
}
