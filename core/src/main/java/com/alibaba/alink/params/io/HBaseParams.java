package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HBaseParams<T> extends WithParams <T> {

	@NameCn("簇值")
	@DescCn("簇值")
	ParamInfo <String> HBASE_FAMILY_NAME = ParamInfoFactory
		.createParamInfo("familyName", String.class)
		.setDescription("hbase family name")
		.setRequired()
		.build();

	default String getFamilyName() {return get(HBASE_FAMILY_NAME);}

	default T setFamilyName(String value) {return set(HBASE_FAMILY_NAME, value);}

	@NameCn("rowkey所在列")
	@DescCn("rowkey所在列")
	ParamInfo <String[]> HBASE_ROWKEY_COLS = ParamInfoFactory
		.createParamInfo("rowKeyCols", String[].class)
		.setDescription("hbase rowkey columns")
		.setRequired()
		.build();

	default String[] getRowKeyCols() {return get(HBASE_ROWKEY_COLS);}

	default T setRowKeyCols(String... value) {return set(HBASE_ROWKEY_COLS, value);}

	@NameCn("HBase表名称")
	@DescCn("HBase表名称")
	ParamInfo <String> HBASE_TABLE_NAME = ParamInfoFactory
		.createParamInfo("tableName", String.class)
		.setDescription("hbase table name")
		.setRequired()
		.build();

	default String getTabeName() {return get(HBASE_TABLE_NAME);}

	default T setTableName(String value) {return set(HBASE_TABLE_NAME, value);}
}
