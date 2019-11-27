package com.alibaba.alink.params.dataproc.vector;

import com.alibaba.alink.params.mapper.SISOMapperParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of vector normalizer.
 */
public interface VectorNormalizeParams<T> extends
	SISOMapperParams<T> {

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
