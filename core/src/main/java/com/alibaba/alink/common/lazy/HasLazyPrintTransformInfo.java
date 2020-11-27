package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLazyPrintTransformInfo<T> extends WithParams <T> {

	ParamInfo <Boolean> LAZY_PRINT_TRANSFORM_DATA_ENABLED = ParamInfoFactory
		.createParamInfo("lazyPrintTransformDataEnabled", Boolean.class)
		.setDescription("Enable lazyPrint of ModelInfo")
		.setHasDefaultValue(false)
		.build();

	ParamInfo <String> LAZY_PRINT_TRANSFORM_DATA_TITLE = ParamInfoFactory
		.createParamInfo("lazyPrintTransformDataTitle", String.class)
		.setDescription("Title of ModelInfo in lazyPrint")
		.setHasDefaultValue(null)
		.build();

	ParamInfo <Integer> LAZY_PRINT_TRANSFORM_DATA_NUM = ParamInfoFactory
		.createParamInfo("lazyPrintTransformDataNum", Integer.class)
		.setDescription("Title of ModelInfo in lazyPrint")
		.setHasDefaultValue(-1)
		.build();

	ParamInfo <Boolean> LAZY_PRINT_TRANSFORM_STAT_ENABLED = ParamInfoFactory
		.createParamInfo("lazyPrintTransformStatEnabled", Boolean.class)
		.setDescription("Enable lazyPrint of ModelInfo")
		.setHasDefaultValue(false)
		.build();

	ParamInfo <String> LAZY_PRINT_TRANSFORM_STAT_TITLE = ParamInfoFactory
		.createParamInfo("lazyPrintTransformStatTitle", String.class)
		.setDescription("Title of ModelInfo in lazyPrint")
		.setHasDefaultValue(null)
		.build();

	default T enableLazyPrintTransformData(int n, String title) {
		this.set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, true);
		this.set(LAZY_PRINT_TRANSFORM_DATA_NUM, n);
		if (null != title) {
			this.set(LAZY_PRINT_TRANSFORM_DATA_TITLE, title);
		}
		return (T) this;
	}

	default T enableLazyPrintTransformData(int n) {
		return enableLazyPrintTransformData(n, null);
	}

	default T enableLazyPrintTransformStat(String title) {
		this.set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, true);
		if (null != title) {
			this.set(LAZY_PRINT_TRANSFORM_STAT_TITLE, title);
		}
		return (T) this;
	}

	default T enableLazyPrintTransformStat() {
		enableLazyPrintTransformStat(null);
		return (T) this;
	}
}
