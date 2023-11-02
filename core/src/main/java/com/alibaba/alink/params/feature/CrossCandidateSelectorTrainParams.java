package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface CrossCandidateSelectorTrainParams<T> extends
	AutoCrossTrainParams <T> {

	@NameCn("特征个数")
	@DescCn("特征个数")
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

	@NameCn("特征候选集")
	@DescCn("特征候选集")
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
