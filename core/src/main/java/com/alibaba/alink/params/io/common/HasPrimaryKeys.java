package com.alibaba.alink.params.io.common;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPrimaryKeys<T> extends WithParams <T> {
	@NameCn("主键列名")
	@DescCn("主键列名")
	ParamInfo <String[]> PRIMARY_KEYS = ParamInfoFactory
		.createParamInfo("primaryKeys", String[].class)
		.setDescription("primary key column names")
		.setRequired()
		.build();

	default String[] getPrimaryKeys() {
		return get(PRIMARY_KEYS);
	}

	default T setPrimaryKeys(String[] value) {
		return set(PRIMARY_KEYS, value);
	}
}