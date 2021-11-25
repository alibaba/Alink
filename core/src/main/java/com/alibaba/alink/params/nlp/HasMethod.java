package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.nlp.Method;
import com.alibaba.alink.params.ParamUtil;

/**
 * Method for keywordsextraction.
 */
public interface HasMethod<T> extends WithParams <T> {
	/**
	 * @cn-name 抽取关键词的方法
	 * @cn 抽取关键词的方法，支持TF_IDF和TEXT_RANK
	 */
	ParamInfo <Method> METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("Method to extract keywords, support TF_IDF and TEXT_RANK")
		.setHasDefaultValue(Method.TEXT_RANK)
		.setAlias(new String[] {"generationType", "algorithmType"})
		.build();

	default Method getMethod() {
		return get(METHOD);
	}

	default T setMethod(Method value) {
		return set(METHOD, value);
	}

	default T setMethod(String value) {
		return set(METHOD, ParamUtil.searchEnum(METHOD, value));
	}
}
