package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * parameters of vector element wise product.
 */
public interface VectorElementwiseProductParams<T> extends
	SISOMapperParams<T> {

	ParamInfo <String> SCALING_VECTOR = ParamInfoFactory
		.createParamInfo("scalingVector", String.class)
		.setDescription("scaling vector with str format")
		.setRequired()
		.build();

	default String getScalingVector() {return get(SCALING_VECTOR);}

	default T setScalingVector(String value) {return set(SCALING_VECTOR, value);}
}
