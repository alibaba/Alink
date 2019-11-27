package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasStringOrderTypeDefaultAsRandom<T> extends WithParams<T> {
	ParamInfo <String> STRING_ORDER_TYPE = ParamInfoFactory
		.createParamInfo("stringOrderType", String.class)
		.setDescription(
			"String order type, one of \"random\", \"frequency_asc\", \"frequency_desc\", \"alphabet_asc\", "
				+ "\"alphabet_desc\".")
		.setHasDefaultValue("random")
		.build();

	default String getStringOrderType() {
		return get(STRING_ORDER_TYPE);
	}

	default T setStringOrderType(String value) {
		return set(STRING_ORDER_TYPE, value);
	}
}
