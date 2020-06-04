package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface FromTripleParams<T> extends
	HasTripleRowCol <T> {

	/**
	 * @cn-name 三元组结构中列信息的列名
	 * @cn 三元组结构中列信息的列名
	 */
	ParamInfo <String> TRIPLE_COL_COL = ParamInfoFactory
		.createParamInfo("tripleColCol", String.class)
		.setDescription("Name of the triple row column")
		.setAlias(new String[]{"tripleCol"})
		.setRequired()
		.build();

	default String getTripleColCol() {
		return get(TRIPLE_COL_COL);
	}

	default T setTripleColCol(String colName) {
		return set(TRIPLE_COL_COL, colName);
	}

	/**
	 * @cn-name 三元组结构中数据信息的列名
	 * @cn 三元组结构中数据信息的列名
	 */
	ParamInfo <String> TRIPLE_VAL_COL = ParamInfoFactory
		.createParamInfo("tripleValCol", String.class)
		.setDescription("Name of the triple row column")
		.setAlias(new String[]{"tripleVal"})
		.setRequired()
		.build();

	default String getTripleValCol() {
		return get(TRIPLE_VAL_COL);
	}

	default T setTripleValCol(String colName) {
		return set(TRIPLE_VAL_COL, colName);
	}
}