package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasFunctionParams<T> extends WithParams <T> {
	@NameCn("自定义参数")
	@DescCn("用户自定义参数，JSON 字典格式的字符串，当入口函数存在名为 \"user_params\" 的参数时传入")
	ParamInfo <String> FUNCTION_PARAMS = ParamInfoFactory
		.createParamInfo("userParams", String.class)
		.setDescription(
			"Parameters in json format, passed in to the entry function when an argument named 'user_params' exists")
		.setHasDefaultValue("{}")
		.build();

	default String getFunctionParams() {
		return get(FUNCTION_PARAMS);
	}

	default T setFunctionParams(String value) {
		return set(FUNCTION_PARAMS, value);
	}
}
