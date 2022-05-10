package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasXGBoostPluginVersion<T> extends WithParams <T> {

	@NameCn("插件版本号")
	@DescCn("插件版本号")
	ParamInfo <String> PLUGIN_VERSION = ParamInfoFactory
		.createParamInfo("pluginVersion", String.class)
		.setDescription("XGBoost plugin version number")
		.setHasDefaultValue("1.5.1")
		.build();

	default T setPluginVersion(String value) {
		return set(PLUGIN_VERSION, value);
	}

	default String getPluginVersion() {
		return get(PLUGIN_VERSION);
	}
}
