package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06.
 */
public interface HasEpsilonDefaultAs0000001<T> extends WithParams <T> {

	@NameCn("收敛阈值")
	@DescCn("迭代方法的终止判断阈值，默认值为 1.0e-6")
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06")
		.setValidator(new MinValidator <>(0.0))
		.setHasDefaultValue(1.0e-6)
		.build();

	default Double getEpsilon() {
		return get(EPSILON);
	}

	default T setEpsilon(Double value) {
		return set(EPSILON, value);
	}
}
