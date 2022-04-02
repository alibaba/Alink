package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDbName<T> extends WithParams <T> {
	@NameCn("数据库名字")
	@DescCn("数据库名字")
	ParamInfo <String> DB_NAME = ParamInfoFactory
		.createParamInfo("dbName", String.class)
		.setDescription("db name")
		.setRequired()
		.build();

	default String getDbName() {return get(DB_NAME);}

	default T setDbName(String value) {return set(DB_NAME, value);}
}
