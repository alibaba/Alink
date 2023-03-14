package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;


public interface HasAppendOriginalData <T> extends WithParams <T> {
	ParamInfo <Boolean> APPEND_ORIGINAL_DATA = ParamInfoFactory
		.createParamInfo("appendOriginalData", Boolean.class)
		.setDescription("append original data or not")
		.setHasDefaultValue(true)
		.build();

	default Boolean getAppendOriginalData() {
		return get(APPEND_ORIGINAL_DATA);
	}

	default T setAppendOriginalData(Boolean value) {
		return set(APPEND_ORIGINAL_DATA, value);
	}

}
