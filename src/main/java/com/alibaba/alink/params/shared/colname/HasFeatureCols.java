package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFeatureCols<T> extends WithParams<T> {
	ParamInfo <String[]> FEATURE_COLS = ParamInfoFactory
		.createParamInfo("featureCols", String[].class)
		.setDescription("Names of the feature columns used for training in the input table")
		.setAlias(new String[] {"featureColNames"})
		.setRequired()
		.build();

	default String[] getFeatureCols() {
		return get(FEATURE_COLS);
	}

	default T setFeatureCols(String... colNames) {
		return set(FEATURE_COLS, colNames);
	}
}
