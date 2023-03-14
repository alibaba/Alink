package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.mapper.MISOMapperParams;

public interface TimeSeriesFunctionParams<T> extends
	MISOMapperParams <T> {
	@NameCn("函数名字")
	@DescCn("函数操作名称, 可取minus（减), minus_abs(减后去绝对值)")
	ParamInfo <FuncName> TIME_SERIES_FUNC_NAME = ParamInfoFactory
		.createParamInfo("funcName", FuncName.class)
		.setDescription("the name of time series function")
		.setRequired()
		.build();

	default FuncName getFuncName() {
		return get(TIME_SERIES_FUNC_NAME);
	}

	default T setFuncName(FuncName value) {
		return set(TIME_SERIES_FUNC_NAME, value);
	}

	default T setFuncName(String value) {
		return set(TIME_SERIES_FUNC_NAME, ParamUtil.searchEnum(TIME_SERIES_FUNC_NAME, value));
	}

	enum FuncName {
		Minus,
		Minus_Abs
	}
}