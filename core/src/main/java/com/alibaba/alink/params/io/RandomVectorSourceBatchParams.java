package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface RandomVectorSourceBatchParams<T> extends WithParams <T> {

	/**
	 * Param "idColName"
	 *
	 * @cn-name id 列名
	 * @cn 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列
	 */
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("id col name")
		.setAlias(new String[] {"idColName"})
		.setHasDefaultValue("alink_id")
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
	 * Param "numRows"
	 *
	 * @cn-name 输出表行数目
	 * @cn 输出表中行的数目，整型
	 */
	ParamInfo <Integer> NUM_ROWS = ParamInfoFactory
		.createParamInfo("numRows", Integer.class)
		.setDescription("num rows")
		.setRequired()
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

	default String getIdCol() {return get(ID_COL);}

	default T setIdCol(String value) {return set(ID_COL, value);}

	default String getOutputCol() {return get(OUTPUT_COL);}

	default T setOutputCol(String value) {return set(OUTPUT_COL, value);}

	default Integer getNumRows() {return get(NUM_ROWS);}

	default T setNumRows(Integer value) {return set(NUM_ROWS, value);}

	default Integer[] getSize() {return get(SIZE);}

	default T setSize(Integer[] value) {return set(SIZE, value);}

	default Double getSparsity() {return get(SPARSITY);}

	default T setSparsity(Double value) {return set(SPARSITY, value);}

}
