package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTableNames<T> extends WithParams <T> {

	@NameCn("数据表名称列表")
	@DescCn("数据表名称列表")
	ParamInfo <String[]> TABLE_NAMES = ParamInfoFactory
		.createParamInfo("tableNames", String[].class)
		.setDescription("table names")
		.build();

	default String[] getTableNames() {
		return get(TABLE_NAMES);
	}

	default T setTableNames(String... value) {
		return set(TABLE_NAMES, value);
	}
}
