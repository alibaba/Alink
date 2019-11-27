package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for RegexTokenizer.
 *
 * @param <T>
 */
public interface RegexTokenizerParams<T> extends
	SISOMapperParams<T> {

	ParamInfo <String> PATTERN = ParamInfoFactory.createParamInfo("pattern", String.class)
		.setDescription("If gaps is true, it's used as a delimiter; If gaps is false, it's used as a token")
		.setOptional()
		.setHasDefaultValue("\\s+")
		.build();
	ParamInfo <Boolean> GAPS = ParamInfoFactory.createParamInfo("gaps", Boolean.class)
		.setDescription("If gaps is true, it splits the document with the given pattern. "
			+ "If gaps is false, it extract the tokens matching the pattern")
		.setOptional()
		.setHasDefaultValue(true)
		.build();
	ParamInfo <Integer> MIN_TOKEN_LENGTH = ParamInfoFactory
		.createParamInfo("minTokenLength", Integer.class)
		.setDescription("The minimum of token length.")
		.setHasDefaultValue(1)
		.build();
	ParamInfo <Boolean> TO_LOWER_CASE = ParamInfoFactory
		.createParamInfo("toLowerCase", Boolean.class)
		.setDescription("If true, transform all the words to lower case。")
		.setHasDefaultValue(true)
		.build();

	default String getPattern() {
		return get(PATTERN);
	}

	default T setPattern(String value) {
		return set(PATTERN, value);
	}

	default Boolean getGaps() {
		return get(GAPS);
	}

	default T setGaps(Boolean value) {
		return set(GAPS, value);
	}

	default Integer getMinTokenLength() {
		return get(MIN_TOKEN_LENGTH);
	}

	default T setMinTokenLength(Integer value) {
		return set(MIN_TOKEN_LENGTH, value);
	}

	default Boolean getToLowerCase() {
		return get(TO_LOWER_CASE);
	}

	default T setToLowerCase(Boolean value) {
		return set(TO_LOWER_CASE, value);
	}

}
