package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * punish factor.
 */
public interface HasLambda<T> extends WithParams <T> {

	@NameCn("希腊字母：lambda")
	@DescCn("惩罚因子，必选")
	ParamInfo <Double> LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("punish factor.")
		.setRequired()
		.build();

	default Double getLambda() {
		return get(LAMBDA);
	}

	default T setLambda(Double value) {
		return set(LAMBDA, value);
	}
}
