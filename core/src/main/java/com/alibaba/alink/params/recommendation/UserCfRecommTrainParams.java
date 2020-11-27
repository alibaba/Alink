package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface UserCfRecommTrainParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	HasSimilarityType <T>,
	HasRateColDefaultAsNull <T> {

	/**
	 * Predictions ignore items below this calc value.
	 */
	ParamInfo <Double> SIMILARITYTHRESHOLD = ParamInfoFactory
		.createParamInfo("similarityThreshold", Double.class)
		.setDescription("threshold")
		.setHasDefaultValue(0.001)
		.build();

	default Double getSimilarityThreshold() {return get(SIMILARITYTHRESHOLD);}

	default T setSimilarityThreshold(Double value) {return set(SIMILARITYTHRESHOLD, value);}

	/**
	 * Number of similar items to store for each item. Default value is 64. Decreasing this decreases the amount of
	 * memory required for the model, but may also decrease the accuracy.
	 */
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("Number of similar items.")
		.setHasDefaultValue(64)
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
