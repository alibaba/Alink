package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasRegSelectorMethod<T> extends WithParams <T> {

	@NameCn("方法")
	@DescCn("方法")
	ParamInfo <Method> REG_SELECTOR_METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("method: fTest and marginalContribution")
		.setOptional()
		.setHasDefaultValue(Method.FTest)
		.build();

	default Method getMethod() {
		return get(REG_SELECTOR_METHOD);
	}

	default T setMethod(Method value) {
		return set(REG_SELECTOR_METHOD, value);
	}

	default T setMethod(String value) {
		return set(REG_SELECTOR_METHOD, ParamUtil.searchEnum(REG_SELECTOR_METHOD, value));
	}

	enum Method {
		FTest,
		MarginalContribution
	}
}
