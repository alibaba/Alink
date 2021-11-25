package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

/**
 * Parameter of correlation method.
 */
public interface HasMethod<T> extends WithParams <T> {

	/**
	 * @cn-name 方法
	 * @cn 方法：包含"PEARSON"和"SPEARMAN"两种，PEARSON。
	 */
	ParamInfo <Method> METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("method: PEARSON, SPEARMAN. default PEARSON")
		.setHasDefaultValue(Method.PEARSON)
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

	enum Method {
		PEARSON,
		SPEARMAN
	}
}
