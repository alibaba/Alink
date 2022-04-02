package com.alibaba.alink.params.shared.delimiter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Params for wordDelimiter.
 */
public interface HasWordDelimiter<T> extends WithParams <T> {
	@NameCn("单词分隔符")
	@DescCn("单词之间的分隔符")
	ParamInfo <String> WORD_DELIMITER = ParamInfoFactory
		.createParamInfo("wordDelimiter", String.class)
		.setDescription("Delimiter of words")
		.setHasDefaultValue(" ")
		.setAlias(new String[] {"delimiter"})
		.build();

	default String getWordDelimiter() {
		return get(WORD_DELIMITER);
	}

	default T setWordDelimiter(String value) {
		return set(WORD_DELIMITER, value);
	}
}
