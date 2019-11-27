package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06.
 *
 */
public interface HasEpsilonDv0000001<T> extends WithParams<T> {

	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06")
		.setHasDefaultValue(1.0e-6)
		.build();

	default Double getEpsilon() {
		return get(EPSILON);
	}

	default T setEpsilon(Double value) {
		return set(EPSILON, value);
	}
}
