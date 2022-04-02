package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Params of the name of the feature column.
 */
public interface HasFeatureColDefaultAsNull<T> extends WithParams <T> {
	@NameCn("特征列名")
	@DescCn("特征列名，默认选最左边的列")
	ParamInfo <String> FEATURE_COL = ParamInfoFactory
		.createParamInfo("featureCol", String.class)
		.setDescription("Name of the feature column")
		.setHasDefaultValue(null)
		.build();

	default String getFeatureCol() {
		return get(FEATURE_COL);
	}

	default T setFeatureCol(String colName) {
		return set(FEATURE_COL, colName);
	}
}