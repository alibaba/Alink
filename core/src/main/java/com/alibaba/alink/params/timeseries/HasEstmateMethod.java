package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasEstmateMethod<T> extends WithParams <T> {

	@NameCn("估计方法")
	@DescCn("估计方法")
	ParamInfo <EstMethod> EST_METHOD = ParamInfoFactory
		.createParamInfo("estMethod", EstMethod.class)
		.setDescription("arima garch method")
		.setHasDefaultValue(EstMethod.CssMle)
		.build();

	default EstMethod getEstMethod() {
		return get(EST_METHOD);
	}

	default T setEstMethod(EstMethod value) {
		return set(EST_METHOD, value);
	}

	default T setEstMethod(String value) {
		return set(EST_METHOD, ParamUtil.searchEnum(EST_METHOD, value));
	}

	enum EstMethod {
		Mom,
		Hr,
		Css,
		CssMle
	}
}
