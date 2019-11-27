package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: number of hash tables.
 */
public interface HasNumHashTables<T> extends
	WithParams<T> {

	ParamInfo <Integer> NUM_HASH_TABLES = ParamInfoFactory
		.createParamInfo("numHashTables", Integer.class)
		.setDescription("The number of hash tables")
		.setHasDefaultValue(1)
		.setAlias(new String[] {"minHashK"})
		.build();

	default Integer getNumHashTables() {
		return get(NUM_HASH_TABLES);
	}

	default T setNumHashTables(Integer value) {
		return set(NUM_HASH_TABLES, value);
	}
}
