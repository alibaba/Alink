package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * the L2-regularized parameter.
 *
 */
public interface HasL2<T> extends WithParams<T> {

	ParamInfo <Double> L_2 = ParamInfoFactory
		.createParamInfo("l2", Double.class)
		.setDescription("the L2-regularized parameter.")
		.setHasDefaultValue(0.0)
		.setAlias(new String[] {"L2"})
		.build();

	default Double getL2() {
		return get(L_2);
	}

	default T setL2(Double value) {
		return set(L_2, value);
	}
}
