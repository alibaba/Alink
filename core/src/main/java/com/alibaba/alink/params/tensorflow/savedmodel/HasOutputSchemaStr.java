package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputSchemaStr<T> extends WithParams <T> {
	/**
	 * @cn-name Schema
	 * @cn Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double"
	 */
	ParamInfo <String> OUTPUT_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("outputSchemaStr", String.class)
		.setDescription("Formatted schema for output")
		.setRequired()
		.setAlias(new String[] {"schemaStr", "schema", "tableSchema"})
		.build();

	default String getOutputSchemaStr() {
		return get(OUTPUT_SCHEMA_STR);
	}

	default T setOutputSchemaStr(String value) {
		return set(OUTPUT_SCHEMA_STR, value);
	}
}
