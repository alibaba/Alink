package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface TfIdfParams<T> extends
	WithParams <T>,
	HasDocIdCol <T> {
	ParamInfo <String> WORD_COL = ParamInfoFactory
		.createParamInfo("wordCol", String.class)
		.setDescription("Name of the word column")
		.setAlias(new String[] {"wordColName"})
		.setRequired()
		.build();

	ParamInfo <String> COUNT_COL = ParamInfoFactory
		.createParamInfo("countCol", String.class)
		.setDescription("Name of the count column")
		.setRequired()
		.setAlias(new String[] {"cntColName", "countColName"})
		.build();

	default String getCountCol() {
		return get(COUNT_COL);
	}

	default T setCountCol(String value) {
		return set(COUNT_COL, value);
	}

	default String getWordCol() {
		return get(WORD_COL);
	}

	default T setWordCol(String value) {
		return set(WORD_COL, value);
	}

}
