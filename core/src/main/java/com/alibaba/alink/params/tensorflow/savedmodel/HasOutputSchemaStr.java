package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputSchemaStr<T> extends WithParams <T> {

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
