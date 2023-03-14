package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface LinearRegStepwiseTrainParams<T> extends WithParams <T>,
	RegressorTrainParams <T> {

	@NameCn("回归统计的方法")
	@DescCn("可取值包括：stepwise，forward，backward")
	ParamInfo <Method> METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("method")
		.setHasDefaultValue(Method.Forward)
		.build();

	default Method getMethod() {
		return get(METHOD);
	}

	default T setMethod(String value) {
		return set(METHOD, ParamUtil.searchEnum(METHOD, value));
	}

	default T setMethod(Method value) {
		return set(METHOD, value);
	}

	enum Method {
		Stepwise,
		Forward,
		Backward
	}
}
