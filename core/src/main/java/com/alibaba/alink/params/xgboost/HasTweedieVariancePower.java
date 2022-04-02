package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTweedieVariancePower<T> extends WithParams <T> {

	@NameCn("学习率")
	@DescCn("学习率")
	ParamInfo <Double> TWEEDIE_VARIANCE_POWER = ParamInfoFactory
		.createParamInfo("tweedieVariancePower", Double.class)
		.setDescription("Parameter that controls the variance of the Tweedie distribution.")
		.setHasDefaultValue(1.5)
		.build();

	default Double getTweedieVariancePower() {
		return get(TWEEDIE_VARIANCE_POWER);
	}

	default T setTweedieVariancePower(Double tweedieVariancePower) {
		return set(TWEEDIE_VARIANCE_POWER, tweedieVariancePower);
	}
}
