package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTextCol<T> extends WithParams<T> {
	ParamInfo <String> TEXT_COL = ParamInfoFactory
		.createParamInfo("textCol", String.class)
		.setDescription("Text Column Name")
		.setHasDefaultValue("text")
		.build();

	default String getTextCol() {
		return get(TEXT_COL);
	}

	default T setTextCol(String textCol) {
		return set(TEXT_COL, textCol);
	}
}
