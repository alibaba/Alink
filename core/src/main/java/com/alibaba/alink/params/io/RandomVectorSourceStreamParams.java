package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface RandomVectorSourceStreamParams<T> extends WithParams <T> {

	/**
	 * Param "idColName"
	 */
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("id col name")
		.setHasDefaultValue("alink_id")
		.setAlias(new String[] {"idColName"})
		.build();
	/**
	 * Param "outputColName"
	 */
	ParamInfo <String> OUTPUT_COL = ParamInfoFactory
		.createParamInfo("outputCol", String.class)
		.setDescription("output col name")
		.setHasDefaultValue("tensor")
		.setAlias(new String[] {"outputColName"})
		.build();
	/**
	 * Param "size"
	 */
	ParamInfo <Integer[]> SIZE = ParamInfoFactory
		.createParamInfo("size", Integer[].class)
		.setDescription("size")
		.setRequired()
		.build();
	/**
	 * Param "maxRows"
	 */
	ParamInfo <Long> MAX_ROWS = ParamInfoFactory
		.createParamInfo("maxRows", Long.class)
		.setDescription("max rows")
		.setRequired()
		.build();
	/**
	 * Param "sparsity"
	 */
	ParamInfo <Double> SPARSITY = ParamInfoFactory
		.createParamInfo("sparsity", Double.class)
		.setDescription("sparsity")
		.setRequired()
		.build();
	/**
	 * Param "timePerSample"
	 */
	ParamInfo <Double> TIME_PER_SAMPLE = ParamInfoFactory
		.createParamInfo("timePerSample", Double.class)
		.setDescription("time per sample")
		.setHasDefaultValue(null)
		.build();

	default String getIdCol() {
		return get(ID_COL);
	}

	default T setIdCol(String value) {
		return set(ID_COL, value);
	}

	default String getOutputCol() {
		return get(OUTPUT_COL);
	}

	default T setOutputCol(String value) {
		return set(OUTPUT_COL, value);
	}

	default Integer[] getSize() {
		return get(SIZE);
	}

	default T setSize(Integer[] value) {
		return set(SIZE, value);
	}

	default Long getMaxRows() {
		return get(MAX_ROWS);
	}

	default T setMaxRows(Long value) {
		return set(MAX_ROWS, value);
	}

	default Double getSparsity() {
		return get(SPARSITY);
	}

	default T setSparsity(Double value) {
		return set(SPARSITY, value);
	}

	default Double getTimePerSample() {
		return get(TIME_PER_SAMPLE);
	}

	default T setTimePerSample(Double value) {
		return set(TIME_PER_SAMPLE, value);
	}

}
