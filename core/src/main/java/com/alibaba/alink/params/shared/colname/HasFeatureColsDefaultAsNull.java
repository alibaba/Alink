package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Params of the names of the feature columns.
 */
public interface HasFeatureColsDefaultAsNull<T> extends WithParams <T> {
	@NameCn("特征列名数组")
	@DescCn("特征列名数组，默认全选")
	ParamInfo <String[]> FEATURE_COLS = ParamInfoFactory
		.createParamInfo("featureCols", String[].class)
		.setDescription("Names of the feature columns.")
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