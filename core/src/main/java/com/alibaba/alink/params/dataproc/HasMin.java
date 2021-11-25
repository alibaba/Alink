package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter min.
 * It is Lower bound after transformation.
 */
public interface HasMin<T> extends WithParams <T> {

	/**
	 * @cn-name 归一化的下界
	 * @cn 归一化的下界
	 */
	ParamInfo <Double> MIN = ParamInfoFactory
		.createParamInfo("min", Double.class)
		.setDescription("Lower bound after transformation.")
		.setHasDefaultValue(0.0)
		.build();

	default Double getMin() {
		return get(MIN);
	}

	default T setMin(Double value) {
		return set(MIN, value);
	}
}
