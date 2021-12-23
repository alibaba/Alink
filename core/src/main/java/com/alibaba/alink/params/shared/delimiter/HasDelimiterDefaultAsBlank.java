package com.alibaba.alink.params.shared.delimiter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDelimiterDefaultAsBlank<T> extends WithParams <T> {
	/**
	 * @cn-name 分隔符
	 * @cn 用来分割字符串
	 */
	ParamInfo <String> DELIMITER = ParamInfoFactory
		.createParamInfo("delimiter", String.class)
		.setDescription("delimiter")
		.setHasDefaultValue(" ")
		.build();

	default String getDelimiter() {return get(DELIMITER);}

	default T setDelimiter(String value) {return set(DELIMITER, value);}
}
