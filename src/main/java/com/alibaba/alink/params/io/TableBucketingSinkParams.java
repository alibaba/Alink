package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface TableBucketingSinkParams<T> extends WithParams<T> {

	/**
	 * Param "batchSize"
	 */
	ParamInfo <Integer> BATCH_SIZE = ParamInfoFactory
		.createParamInfo("batchSize", Integer.class)
		.setDescription("batch size")
		.setHasDefaultValue(-1)
		.build();
	/**
	 * Param "batchRolloverInterval"
	 */
	ParamInfo <Long> BATCH_ROLLOVER_INTERVAL = ParamInfoFactory
		.createParamInfo("batchRolloverInterval", Long.class)
		.setDescription("batch rollover interval")
		.setHasDefaultValue(-1L)
		.build();
	/**
	 * Param "isOverwrite
	 */
	ParamInfo <Boolean> IS_OVER_WRITE = ParamInfoFactory
		.createParamInfo("isOverWrite", Boolean.class)
		.setDescription("")
		.setRequired()
		.build();

	default Integer getBatchSize() {
		return get(BATCH_SIZE);
	}

	default T setBatchSize(Integer value) {
		return set(BATCH_SIZE, value);
	}

	default Long getBatchRolloverInterval() {
		return get(BATCH_ROLLOVER_INTERVAL);
	}

	default T setBatchRolloverInterval(Long value) {
		return set(BATCH_ROLLOVER_INTERVAL, value);
	}

	default Boolean getIsOverWrite() {
		return get(IS_OVER_WRITE);
	}

	default T setIsOverWrite(Boolean value) {
		return set(IS_OVER_WRITE, value);
	}
}
