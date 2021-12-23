package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSchemaStrDefaultAsNull<T> extends WithParams <T> {
	/**
	 * @cn-name Schema
	 * @cn Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double"
	 */
	ParamInfo <String> SCHEMA_STR = ParamInfoFactory
		.createParamInfo("schemaStr", String.class)
		.setDescription("Formatted schema")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"schema", "tableSchema"})
		.build();

	default String getSchemaStr() {
		return get(SCHEMA_STR);
	}

	default T setSchemaStr(String value) {
		return set(SCHEMA_STR, value);
	}
}
