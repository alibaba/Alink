package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params: When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges!
 */
public interface HasEpsilonDv00001<T> extends WithParams <T> {
	/**
	 * @cn-name 收敛阈值
	 * @cn 当两轮迭代的中心点距离小于epsilon时，算法收敛。
	 * @param <T>
	 */
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription(
			"When the distance between two rounds of centers is lower than epsilon, we consider the algorithm "
				+ "converges!")
		.setHasDefaultValue(1.0e-4)
		.build();

	default Double getEpsilon() {return get(EPSILON);}

	default T setEpsilon(Double value) {return set(EPSILON, value);}
}
