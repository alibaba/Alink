package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEntryFunction<T> extends WithParams <T> {
	@NameCn("入口函数")
	@DescCn("入口函数路径，需要包含完整的包名以及函数名。例如，入口函数可以通过 \"from a.b import c\" 导入时，那么应该填写 \"a.b.c\"。")
	ParamInfo <String> ENTRY_FUNCTION = ParamInfoFactory
		.createParamInfo("entryFunction", String.class)
		.setDescription("Entry Function including package names and function name")
		.setRequired()
		.build();

	default String getEntryFunction() {
		return get(ENTRY_FUNCTION);
	}

	default T setEntryFunction(String value) {
		return set(ENTRY_FUNCTION, value);
	}
}
