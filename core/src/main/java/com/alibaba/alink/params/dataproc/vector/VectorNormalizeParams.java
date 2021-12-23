package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * parameters of vector normalizer.
 */
public interface VectorNormalizeParams<T> extends SISOMapperParams <T> {

	/**
	 * @cn-name 范数的阶
	 * @cn 范数的阶，默认2
	 */
	ParamInfo <Double> P = ParamInfoFactory
		.createParamInfo("p", Double.class)
		.setDescription("number of degree.")
		.setHasDefaultValue(2.0)
		.build();

	default Double getP() {
		return get(P);
	}

	default T setP(Double value) {
		return set(P, value);
	}
}