package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Params for StopWordsRemover.
 */
public interface StopWordsRemoverParams<T> extends SISOMapperParams <T> {

	@NameCn("是否大小写敏感")
	@DescCn("大小写敏感")
	ParamInfo <Boolean> CASE_SENSITIVE = ParamInfoFactory
		.createParamInfo("caseSensitive", Boolean.class)
		.setDescription("If true, do a case sensitive comparison over the stop words")
		.setHasDefaultValue(false)
		.build();
	@NameCn("用户自定义停用词表")
	@DescCn("用户自定义停用词表")
	ParamInfo <String[]> STOP_WORDS = ParamInfoFactory
		.createParamInfo("stopWords", String[].class)
		.setDescription("User defined stop words list。")
		.setHasDefaultValue(null)
		.build();

	default Boolean getCaseSensitive() {
		return get(CASE_SENSITIVE);
	}

	default T setCaseSensitive(Boolean value) {
		return set(CASE_SENSITIVE, value);
	}

	default String[] getStopWords() {
		return get(STOP_WORDS);
	}

	default T setStopWords(String... value) {
		return set(STOP_WORDS, value);
	}
}
