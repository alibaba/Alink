package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface FromTripleParams<T> extends
	HasTripleRowCol <T> {

	@NameCn("三元组结构中列信息的列名")
	@DescCn("三元组结构中列信息的列名")
	ParamInfo <String> TRIPLE_COLUMN_COL = ParamInfoFactory
		.createParamInfo("tripleColumnCol", String.class)
		.setDescription("Name of the triple column col")
		.setAlias(new String[] {"tripleCol", "tripleColCol"})
		.setRequired()
		.build();

	default String getTripleColumnCol() {
		return get(TRIPLE_COLUMN_COL);
	}

	default T setTripleColumnCol(String colName) {
		return set(TRIPLE_COLUMN_COL, colName);
	}

	@NameCn("三元组结构中数据信息的列名")
	@DescCn("三元组结构中数据信息的列名")
	ParamInfo <String> TRIPLE_VALUE_COL = ParamInfoFactory
		.createParamInfo("tripleValueCol", String.class)
		.setDescription("Name of the triple value column")
		.setAlias(new String[] {"tripleVal", "tripleValCol"})
		.setRequired()
		.build();

	default String getTripleValueCol() {
		return get(TRIPLE_VALUE_COL);
	}

	default T setTripleValueCol(String colName) {
		return set(TRIPLE_VALUE_COL, colName);
	}
}