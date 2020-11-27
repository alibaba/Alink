package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLazyPrintModelInfo<T> extends WithParams <T> {

	ParamInfo <Boolean> LAZY_PRINT_MODEL_INFO_ENABLED = ParamInfoFactory
		.createParamInfo("lazyPrintModelInfoEnabled", Boolean.class)
		.setDescription("Enable lazyPrint of ModelInfo")
		.setHasDefaultValue(false)
		.build();

	ParamInfo <String> LAZY_PRINT_MODEL_INFO_TITLE = ParamInfoFactory
		.createParamInfo("lazyPrintModelInfoTitle", String.class)
		.setDescription("Title of ModelInfo in lazyPrint")
		.setHasDefaultValue(null)
		.build();

	default T enableLazyPrintModelInfo(String title) {
		this.set(LAZY_PRINT_MODEL_INFO_ENABLED, true);
		if (null != title) {
			this.set(LAZY_PRINT_MODEL_INFO_TITLE, title);
		}
		return (T) this;
	}

	default T enableLazyPrintModelInfo() {
		return enableLazyPrintModelInfo(null);
	}
}
