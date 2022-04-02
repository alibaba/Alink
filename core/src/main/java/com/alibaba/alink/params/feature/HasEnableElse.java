package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEnableElse<T> extends WithParams <T> {
	@NameCn("预测时是否考虑未出现的元素")
	@DescCn("预测时是否考虑未出现的元素，默认true")
	ParamInfo <Boolean> ENABLE_ELSE = ParamInfoFactory
		.createParamInfo("enableElse", Boolean.class)
		.setDescription("enableElse")
		.setHasDefaultValue(Boolean.TRUE)
		.build();

	default Boolean getEnableElse() {
		return get(ENABLE_ELSE);
	}

	default T setEnableElse(Boolean value) {
		return set(ENABLE_ELSE, value);
	}
}
