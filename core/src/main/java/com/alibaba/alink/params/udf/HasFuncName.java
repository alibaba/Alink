package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * the name of a registered UDF/UDTF function in the table environment
 */
public interface HasFuncName<T> extends WithParams <T> {

	/**
	 * @cn-name 函数名
	 * @cn 函数名
	 */
	ParamInfo <String> FUNC_NAME = ParamInfoFactory
		.createParamInfo("funcName", String.class)
		.setDescription("function name")
		.setHasDefaultValue(null)
		.setOptional()
		.build();

	default T setFuncName(String clsName) {
		return set(FUNC_NAME, clsName);
	}

	default String getFuncName() {
		return get(FUNC_NAME);
	}
}
