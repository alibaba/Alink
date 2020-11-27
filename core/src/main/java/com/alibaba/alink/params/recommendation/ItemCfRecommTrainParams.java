package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface ItemCfRecommTrainParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	HasSimilarityType <T>,
	HasRateColDefaultAsNull <T>,
	HasSimilarityThresholdDefaultAsEN4 <T> {

	/**
	 * Number of similar items to store for each item. Default value is 64. Decreasing this decreases the amount of
	 * memory required for the model, but may also decrease the accuracy.
	 */
	ParamInfo <Integer> MAX_NEIGHBOR_NUMBER = ParamInfoFactory
		.createParamInfo("maxNeighborNumber", Integer.class)
		.setDescription("Number of similar items.")
		.setHasDefaultValue(64)
		.build();

	default Integer getMaxNeighborNumber() {
		return get(MAX_NEIGHBOR_NUMBER);
	}

	default T setMaxNeighborNumber(Integer value) {
		return set(MAX_NEIGHBOR_NUMBER, value);
	}
}
