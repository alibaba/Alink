package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Params for RegexTokenizer.
 *
 * @param <T>
 */
public interface RegexTokenizerParams<T> extends SISOMapperParams <T> {

	/**
	 * @cn-name 分隔符/正则匹配符
	 * @cn 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。
	 */
	ParamInfo <String> PATTERN = ParamInfoFactory.createParamInfo("pattern", String.class)
		.setDescription("If gaps is true, it's used as a delimiter; If gaps is false, it's used as a token")
		.setOptional()
		.setHasDefaultValue("\\s+")
		.build();
	/**
	 * @cn-name 切分/匹配
	 * @cn 如果gaps为True，pattern用于切分文档；如果gaps为False，会提取出匹配pattern的词。
	 */
	ParamInfo <Boolean> GAPS = ParamInfoFactory.createParamInfo("gaps", Boolean.class)
		.setDescription("If gaps is true, it splits the document with the given pattern. "
			+ "If gaps is false, it extract the tokens matching the pattern")
		.setOptional()
		.setHasDefaultValue(true)
		.build();
	/**
	 * @cn-name 词语最短长度
	 * @cn 词语的最短长度，小于这个值的词语会被过滤掉
	 */
	ParamInfo <Integer> MIN_TOKEN_LENGTH = ParamInfoFactory
		.createParamInfo("minTokenLength", Integer.class)
		.setDescription("The minimum of token length.")
		.setHasDefaultValue(1)
		.build();
	/**
	 * @cn-name 是否转换为小写
	 * @cn 转换为小写
	 */
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
