package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface PySqlCmdParams<T> extends WithParams <T> {

	@NameCn("SQL 语句")
	@DescCn("SQL 语句，组件需要提前注册，在语句中用标识符表示")
	ParamInfo <String> COMMAND = ParamInfoFactory
		.createParamInfo("command", String.class)
		.setDescription("SQL statement where operators need to be registered in prior and represented as identifiers.")
		.setRequired()
		.build();

	default String getCommand() {return get(COMMAND);}

	default T setCommand(String value) {return set(COMMAND, value);}
}
