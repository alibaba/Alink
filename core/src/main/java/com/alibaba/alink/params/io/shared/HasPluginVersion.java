package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPluginVersion<T> extends WithParams<T> {

	ParamInfo <String> PLUGIN_VERSION = ParamInfoFactory
		.createParamInfo("pluginVersion", String.class)
		.setDescription("Plugin version number")
		.setRequired()
		.build();

	default T setPluginVersion(String value) {
		return set(PLUGIN_VERSION, value);
	}

	default String getPluginVersion() {
		return get(PLUGIN_VERSION);
	}
}
