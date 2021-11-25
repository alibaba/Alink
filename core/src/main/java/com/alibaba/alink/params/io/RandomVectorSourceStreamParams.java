package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface RandomVectorSourceStreamParams<T> extends WithParams <T> {

	/**
	 * Param "idColName"
	 *
	 * @cn-name id 列名
	 * @cn 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列
	 */
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("id col name")
		.setHasDefaultValue("alink_id")
		.setAlias(new String[] {"idColName"})
		.build();
	/**
	 * Param "outputColName"
	 *
	 * @cn-name 输出列名
	 * @cn 输出随机生成的数据存储列名
	 */
	ParamInfo <String> OUTPUT_COL = ParamInfoFactory
		.createParamInfo("outputCol", String.class)
		.setDescription("output col name")
		.setHasDefaultValue("tensor")
		.setAlias(new String[] {"outputColName"})
		.build();
	/**
	 * Param "size"
	 *
	 * @cn-name 张量size
	 * @cn 整型数组，张量的size
	 */
	ParamInfo <Integer[]> SIZE = ParamInfoFactory
		.createParamInfo("size", Integer[].class)
		.setDescription("size")
		.setRequired()
		.build();
	/**
	 * Param "maxRows"
	 *
	 * @cn-name 最大行数
	 * @cn 输出数据流的行数目的最大值
	 */
	ParamInfo <Long> MAX_ROWS = ParamInfoFactory
		.createParamInfo("maxRows", Long.class)
		.setDescription("max rows")
		.setRequired()
		.build();
	/**
	 * Param "sparsity"
	 *
	 * @cn-name 稀疏度
	 * @cn 非零元素在所有张量数据中的占比
	 */
	ParamInfo <Double> SPARSITY = ParamInfoFactory
		.createParamInfo("sparsity", Double.class)
		.setDescription("sparsity")
		.setRequired()
		.build();
	/**
	 * Param "timePerSample"
	 *
	 * @cn-name 稀疏度
	 * @cn 整型数组，张量的size
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
