package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasBinarySelectorMethod<T> extends WithParams <T> {
	@NameCn("方法")
	@DescCn("方法")
	ParamInfo <Method> BINARY_METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("method: ScoreTest and MarginalContribution")
		.setOptional()
		.setHasDefaultValue(Method.ScoreTest)
		.build();

	default Method getMethod() {
		return get(BINARY_METHOD);
	}

	default T setMethod(Method value) {
		return set(BINARY_METHOD, value);
	}

	default T setMethod(String value) {
		return set(BINARY_METHOD, ParamUtil.searchEnum(BINARY_METHOD, value));
	}

	enum Method {
		ScoreTest,
		MarginalContribution
	}
}
