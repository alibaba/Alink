package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinConfidenceDefaultAs005<T> extends WithParams <T> {
	/**
	 * @cn-name 最小置信度
	 * @cn 最小置信度，同时包含X和Y的样本与包含X的样本之比，反映了当样本中包含项集X时，项集Y同时出现的概率。
	 */
	ParamInfo <Double> MIN_CONFIDENCE = ParamInfoFactory
		.createParamInfo("minConfidence", Double.class)
		.setDescription("Minimum confidence")
		.setHasDefaultValue(0.05)
		.build();

	default Double getMinConfidence() {
		return get(MIN_CONFIDENCE);
	}

	default T setMinConfidence(Double value) {
		return set(MIN_CONFIDENCE, value);
	}
}
