package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.common.similarity.SimilarityUtil;
import com.alibaba.alink.params.nlp.HasWindowSize_2;

/**
 * Params of StringTextExact.
 */
public interface StringTextExactParams<T> extends
	HasWindowSize_2 <T> {
	ParamInfo <Double> LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("punish factor.")
		.setHasDefaultValue(SimilarityUtil.LAMBDA)
		.build();

	default Double getLambda() {
		return get(LAMBDA);
	}

	default T setLambda(Double value) {
		return set(LAMBDA, value);
	}
}
