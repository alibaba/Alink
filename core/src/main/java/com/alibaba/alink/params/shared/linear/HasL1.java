package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

/**
 * the L1-regularized parameter.
 */
public interface HasL1<T> extends WithParams <T> {

	/**
	 * @cn-name L1 正则化系数
	 * @cn L1 正则化系数，默认为0。
	 */
	ParamInfo <Double> L_1 = ParamInfoFactory
		.createParamInfo("l1", Double.class)
		.setDescription("the L1-regularized parameter.")
		.setHasDefaultValue(0.0)
		.setValidator(new MinValidator <>(0.0))
		.setAlias(new String[] {"L1"})
		.build();

	default Double getL1() {
		return get(L_1);
	}

	default T setL1(Double value) {
		return set(L_1, value);
	}
}
