package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasFuncName<T> extends WithParams <T> {
	/**
	 * @cn-name 函数名字
	 * @cn 函数操作名称, 可取max（最大值）, min（最小值）, argMax（最大值索引）, argMin（最小值索引）
	 */
	ParamInfo <FuncName> FUNC_NAME = ParamInfoFactory
		.createParamInfo("funcName", FuncName.class)
		.setDescription("the name of vecor function")
		.setRequired()
		.build();

	default FuncName getFuncName() {
		return get(FUNC_NAME);
	}

	default T setFuncName(FuncName value) {
		return set(FUNC_NAME, value);
	}

	default T setFuncName(String value) {
		return set(FUNC_NAME, ParamUtil.searchEnum(FUNC_NAME, value));
	}

	enum FuncName {
		Max,
		Min,
		ArgMax,
		ArgMin,
		Scale,
		NormL2,
		NormL1,
		NormL2Square,
		Normalize
	}
}