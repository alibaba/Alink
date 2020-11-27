package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasKvColDelimiterDefaultAsComma<T> extends WithParams <T> {
	ParamInfo <String> KV_COL_DELIMITER = ParamInfoFactory
		.createParamInfo("kvColDelimiter", String.class)
		.setDescription("Delimiter used between key-value pairs when data in the input table is in sparse format")
		.setAlias(new String[] {"colDelimiter"})
		.setHasDefaultValue(",")
		.build();

	default String getKvColDelimiter() {
		return get(KV_COL_DELIMITER);
	}

	default T setKvColDelimiter(String value) {
		return set(KV_COL_DELIMITER, value);
	}
}
