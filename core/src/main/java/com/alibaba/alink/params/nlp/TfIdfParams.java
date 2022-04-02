package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface TfIdfParams<T> extends
	WithParams <T>,
	HasDocIdCol <T> {
	@NameCn("单词列")
	@DescCn("单词列名")
	ParamInfo <String> WORD_COL = ParamInfoFactory
		.createParamInfo("wordCol", String.class)
		.setDescription("Name of the word column")
		.setAlias(new String[] {"wordColName"})
		.setRequired()
		.build();

	@NameCn("词频列")
	@DescCn("词频列名")
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
