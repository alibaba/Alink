package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * the L1-regularized parameter.
 *
 */
public interface HasL1<T> extends WithParams<T> {

	ParamInfo <Double> L_1 = ParamInfoFactory
		.createParamInfo("l1", Double.class)
		.setDescription("the L1-regularized parameter.")
		.setHasDefaultValue(0.0)
		.setAlias(new String[] {"L1"})
		.build();

	default Double getL1() {
		return get(L_1);
	}

	default T setL1(Double value) {
		return set(L_1, value);
	}
}
