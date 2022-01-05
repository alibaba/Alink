package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface UserCfRecommTrainParams<T> extends
	HasUserCol <T>,
	HasItemCol <T>,
	HasSimilarityType <T>,
	HasRateColDefaultAsNull <T>,
	HasSimilarityThresholdDefaultAsEN4<T> {

	/**
	 * Number of similar items to store for each item. Default value is 64. Decreasing this decreases the amount of
	 * memory required for the model, but may also decrease the accuracy.
	 *
	 * @cn-name 相似集合元素数目
	 * @cn 相似集合元素数目
	 */
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("Number of similar items.")
		.setHasDefaultValue(64)
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
