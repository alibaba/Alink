package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Params: When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges!
 */
public interface HasEpsilonDefaultAs00001<T> extends WithParams <T> {
	@NameCn("收敛阈值")
	@DescCn("当两轮迭代的中心点距离小于epsilon时，算法收敛。")
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
