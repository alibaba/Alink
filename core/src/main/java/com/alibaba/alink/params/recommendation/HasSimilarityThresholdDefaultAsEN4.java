package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSimilarityThresholdDefaultAsEN4<T> extends WithParams <T> {

	/**
	 * Predictions ignore items below this calc value.
	 */
	@NameCn("相似阈值")
	@DescCn("只有大于该阈值的Object才会被计算")
	ParamInfo <Double> SIMILARITY_THRESHOLD = ParamInfoFactory
		.createParamInfo("similarityThreshold", Double.class)
		.setDescription("threshold")
		.setHasDefaultValue(1e-4)
		.build();

	default Double getSimilarityThreshold() {
		return get(SIMILARITY_THRESHOLD);
	}

	default T setSimilarityThreshold(Double value) {
		return set(SIMILARITY_THRESHOLD, value);
	}
}
