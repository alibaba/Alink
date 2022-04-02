package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Constant item regular number.
 */
public interface HasLambda0DefaultAs0<T> extends WithParams <T> {
	@NameCn("常数项正则化系数")
	@DescCn("常数项正则化系数")
	ParamInfo <Double> LAMBDA_0 = ParamInfoFactory
		.createParamInfo("lambda0", Double.class)
		.setDescription("lambda0")
		.setHasDefaultValue(0.0)
		.build();

	default Double getLambda0() {
		return get(LAMBDA_0);
	}

	default T setLambda0(Double value) {
		return set(LAMBDA_0, value);
	}

}
