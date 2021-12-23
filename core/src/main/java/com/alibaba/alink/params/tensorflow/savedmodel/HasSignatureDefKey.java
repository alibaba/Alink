package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSignatureDefKey<T> extends WithParams <T> {
	/**
	 * @cn-name signature标签
	 * @cn signature标签
	 */
	ParamInfo <String> SIGNATURE_DEF_KEY = ParamInfoFactory
		.createParamInfo("signatureDefKey", String.class)
		.setDescription("signature def key")
		.setHasDefaultValue("serving_default")
		.build();

	default String getSignatureDefKey() {
		return get(SIGNATURE_DEF_KEY);
	}

	default T setSignatureDefKey(String value) {
		return set(SIGNATURE_DEF_KEY, value);
	}
}
