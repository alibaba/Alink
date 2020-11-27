package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLazyPrintTrainInfo<T> extends WithParams <T> {

	ParamInfo <Boolean> LAZY_PRINT_TRAIN_INFO_ENABLED = ParamInfoFactory
		.createParamInfo("lazyPrintTrainInfoEnabled", Boolean.class)
		.setDescription("Enable lazyPrint of TrainInfo")
		.setHasDefaultValue(false)
		.build();

	ParamInfo <String> LAZY_PRINT_TRAIN_INFO_TITLE = ParamInfoFactory
		.createParamInfo("lazyPrintTrainInfoTitle", String.class)
		.setDescription("Title of TrainInfo in lazyPrint")
		.setHasDefaultValue(null)
		.build();

	default T enableLazyPrintTrainInfo(String title) {
		this.set(LAZY_PRINT_TRAIN_INFO_ENABLED, true);
		if (null != title) {
			this.set(LAZY_PRINT_TRAIN_INFO_TITLE, title);
		}
		return (T) this;
	}

	default T enableLazyPrintTrainInfo() {
		return enableLazyPrintTrainInfo(null);
	}
}
