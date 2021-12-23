package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params of the names of the feature columns used for training in the input table.
 */
public interface HasFeatureColsDefaultAsNull<T> extends WithParams <T> {
	/**
	 * @cn-name 特征列名数组
	 * @cn 特征列名数组，默认全选
	 */
	ParamInfo <String[]> FEATURE_COLS = ParamInfoFactory
		.createParamInfo("featureCols", String[].class)
		.setDescription("Names of the feature columns used for training in the input table")
		.setAlias(new String[] {"featureColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getFeatureCols() {
		return get(FEATURE_COLS);
	}

	default T setFeatureCols(String... colNames) {
		return set(FEATURE_COLS, colNames);
	}
}