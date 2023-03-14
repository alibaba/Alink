package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasScaledValue<T> extends WithParams <T> {
	@NameCn("分数基准点")
	@DescCn("分数基准点")
	ParamInfo <Double> SCALED_VALUE = ParamInfoFactory
		.createParamInfo("scaledValue", Double.class)
		.setDescription("scaledValue")
		.setHasDefaultValue(null)
		.build();

	default Double getScaledValue() {
		return get(SCALED_VALUE);
	}

	default T setScaledValue(Double value) {
		return set(SCALED_VALUE, value);
	}
}
