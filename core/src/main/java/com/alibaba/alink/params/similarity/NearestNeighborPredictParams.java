package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Parameters for nearest neighbor predict.
 */
public interface NearestNeighborPredictParams<T> extends SISOMapperParams <T> {

	ParamInfo <Double> RADIUS = ParamInfoFactory
		.createParamInfo("radius", Double.class)
		.setDescription("radius")
		.setHasDefaultValue(null)
		.build();

	default Double getRadius() {return get(RADIUS);}

	default T setRadius(Double value) {return set(RADIUS, value);}

	ParamInfo <Integer> TOP_N = ParamInfoFactory
		.createParamInfo("topN", Integer.class)
		.setDescription("top n")
		.setHasDefaultValue(null)
		.setValidator(new MinValidator <>(1).setNullValid(true))
		.build();

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}
}
