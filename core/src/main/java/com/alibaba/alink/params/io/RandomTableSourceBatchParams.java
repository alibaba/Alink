package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface RandomTableSourceBatchParams<T> extends WithParams <T> {

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
		.setHasDefaultValue(null)
		.build();
	/**
	 * Param "outputColConfs"
	 *
	 * @cn-name 列配置信息
	 * @cn 表示每一列的数据分布配置信息
	 */
	ParamInfo <String> OUTPUT_COL_CONFS = ParamInfoFactory
		.createParamInfo("outputColConfs", String.class)
		.setDescription("output col confs")
		.setHasDefaultValue(null)
		.build();
	/**
	 * Param "outputColNames"
	 *
	 * @cn-name 输出列名数组
	 * @cn 字符串数组，当参数不设置时，算法自动生成
	 */
	ParamInfo <String[]> OUTPUT_COLS = ParamInfoFactory
		.createParamInfo("outputCols", String[].class)
		.setDescription("output col names")
		.setAlias(new String[] {"outputColNames"})
		.setHasDefaultValue(null)
		.build();
	/**
	 * Param "numCols"
	 *
	 * @cn-name 输出表列数目
	 * @cn 输出表中列的数目，整型
	 */
	ParamInfo <Integer> NUM_COLS = ParamInfoFactory
		.createParamInfo("numCols", Integer.class)
		.setDescription("num cols")
		.setRequired()
		.build();
	/**
	 * Param "numRows"
	 *
	 * @cn-name 输出表行数目
	 * @cn 输出表中行的数目，整型
	 */
	ParamInfo <Long> NUM_ROWS = ParamInfoFactory
		.createParamInfo("numRows", Long.class)
		.setDescription("num rows")
		.setRequired()
		.build();

	default String getIdCol() {return get(ID_COL);}

	default T setIdCol(String value) {return set(ID_COL, value);}

	default String getOutputColConfs() {return get(OUTPUT_COL_CONFS);}

	default T setOutputColConfs(String value) {return set(OUTPUT_COL_CONFS, value);}

	default String[] getOutputCols() {return get(OUTPUT_COLS);}

	default T setOutputCols(String... value) {return set(OUTPUT_COLS, value);}

	default Integer getNumCols() {return get(NUM_COLS);}

	default T setNumCols(Integer value) {return set(NUM_COLS, value);}

	default Long getNumRows() {return get(NUM_ROWS);}

	default T setNumRows(Long value) {return set(NUM_ROWS, value);}

}
