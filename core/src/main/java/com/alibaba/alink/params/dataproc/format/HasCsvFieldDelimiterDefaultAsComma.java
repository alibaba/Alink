package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasCsvFieldDelimiterDefaultAsComma<T> extends WithParams<T> {
	/**
	 * @cn-name 字段分隔符
	 * @cn 字段分隔符
	 */
	ParamInfo <String> CSV_FIELD_DELIMITER = ParamInfoFactory
		.createParamInfo("csvFieldDelimiter", String.class)
		.setDescription("Field delimiter")
		.setHasDefaultValue(",")
		.build();

	default String getCsvFieldDelimiter() {
		return get(CSV_FIELD_DELIMITER);
	}

	default T setCsvFieldDelimiter(String value) {
		return set(CSV_FIELD_DELIMITER, value);
	}
}
