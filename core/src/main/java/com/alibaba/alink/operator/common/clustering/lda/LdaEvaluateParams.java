package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface LdaEvaluateParams<T> extends WithParams <T> {
	/**
	 * @cn 对数困惑度的值
	 */
	ParamInfo <Double> LOG_PERPLEXITY = ParamInfoFactory
		.createParamInfo("logPerplexity", Double.class)
		.setDescription("logPerplexity")
		.build();

	/**
	 * @cn 对数相似度的值
	 */
	ParamInfo <Double> LOG_LIKELIHOOD = ParamInfoFactory
		.createParamInfo("logLikelihood", Double.class)
		.setDescription("logLikelihood")
		.build();
}