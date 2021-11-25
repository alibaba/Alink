package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasKvValDelimiterDefaultAsColon<T> extends WithParams <T> {
	/**
	 * @cn-name 分隔符
	 * @cn 当输入数据为稀疏格式时，key和value的分割符
	 */
	ParamInfo <String> KV_VAL_DELIMITER = ParamInfoFactory
		.createParamInfo("kvValDelimiter", String.class)
		.setDescription("Delimiter used between keys and values when data in the input table is in sparse format")
		.setAlias(new String[] {"valDelimiter"})
		.setHasDefaultValue(":")
		.build();

	default String getKvValDelimiter() {
		return get(KV_VAL_DELIMITER);
	}

	default T setKvValDelimiter(String value) {
		return set(KV_VAL_DELIMITER, value);
	}
}
