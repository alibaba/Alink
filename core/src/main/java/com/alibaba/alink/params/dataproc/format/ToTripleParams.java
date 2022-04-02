package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface ToTripleParams<T> extends WithParams <T> {

	@NameCn("三元组结构中列信息和数据信息的Schema")
	@DescCn("三元组结构中列信息和数据信息的Schema")
	ParamInfo <String> TRIPLE_COLUMN_VALUE_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("tripleColumnValueSchemaStr", String.class)
		.setDescription("Schema string of the triple's column and value column")
		.setRequired()
		.setAlias(new String[] {"tripleColValSchemaStr"})
		.build();

	default String getTripleColumnValueSchemaStr() {
		return get(TRIPLE_COLUMN_VALUE_SCHEMA_STR);
	}

	default T setTripleColumnValueSchemaStr(String colName) {
		return set(TRIPLE_COLUMN_VALUE_SCHEMA_STR, colName);
	}

	@NameCn("算法保留列名")
	@DescCn("算法保留列")
	ParamInfo <String[]> RESERVED_COLS = ParamInfoFactory
		.createParamInfo("reservedCols", String[].class)
		.setDescription("Names of the columns to be retained in the output table")
		.setAlias(new String[] {"keepColNames"})
		.setHasDefaultValue(new String[0])
		.build();

	default String[] getReservedCols() {
		return get(RESERVED_COLS);
	}

	default T setReservedCols(String... colNames) {
		return set(RESERVED_COLS, colNames);
	}

}