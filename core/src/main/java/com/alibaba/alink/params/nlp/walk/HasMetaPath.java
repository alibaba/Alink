package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMetaPath<T> extends WithParams <T> {
	/**
	 * @cn-name 游走的模式
	 * @cn 一般为用字符串表示，例如 "ABDFA"
	 */
	ParamInfo <String> META_PATH = ParamInfoFactory
		.createParamInfo("metaPath", String.class)
		.setDescription("meta path")
		.setRequired()
		.build();

	default String getMetaPath() {return get(META_PATH);}

	default T setMetaPath(String value) {return set(META_PATH, value);}
}
