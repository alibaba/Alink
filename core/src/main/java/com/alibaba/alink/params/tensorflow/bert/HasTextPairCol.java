package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTextPairCol<T> extends WithParams <T> {
	/**
	 * @cn 文本对列
	 * @cn-name 文本对列
	 */
	ParamInfo <String> TEXT_PAIR_COL = ParamInfoFactory
		.createParamInfo("textPairCol", String.class)
		.setDescription("Name of the text pair column")
		.setAlias(new String[] {"rightSentenceCol"})
		.setRequired()
		.build();

	default String getTextPairCol() {
		return get(TEXT_PAIR_COL);
	}

	default T setTextPairCol(String colName) {
		return set(TEXT_PAIR_COL, colName);
	}
}
