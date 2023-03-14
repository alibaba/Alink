package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;


public interface CrossCandidateSelectorTrainParams<T> extends
	AutoCrossTrainParams <T> {

	ParamInfo <Integer> CROSS_FEATURE_NUMBER = ParamInfoFactory
		.createParamInfo("crossFeatureNumber", Integer.class)
		.setDescription("number of crossed features.")
		.setRequired()
		.build();

	default Integer getCrossFeatureNumber() {
		return get(CROSS_FEATURE_NUMBER);
	}

	default T setCrossFeatureNumber(Integer colNames) {
		return set(CROSS_FEATURE_NUMBER, colNames);
	}

	ParamInfo <String[]> FEATURE_CANDIDATES = ParamInfoFactory
		.createParamInfo("featureCandidates", String[].class)
		.setDescription("feature candidates.")
		.setRequired()
		.build();

	default String[] getFeatureCandidates() {
		return get(FEATURE_CANDIDATES);
	}

	default T setFeatureCandidates(String[] colNames) {
		return set(FEATURE_CANDIDATES, colNames);
	}

}
