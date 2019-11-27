package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDistanceThreshold<T> extends WithParams<T> {
	ParamInfo <Double> DISTANCE_THRESHOLD = ParamInfoFactory
		.createParamInfo("distanceThreshold", Double.class)
		.setDescription("distance threshold")
		.setHasDefaultValue(Double.MAX_VALUE)
		.setAlias(new String[] {"threshold"})
		.build();

	default Double getDistanceThreshold() {return get(DISTANCE_THRESHOLD);}

	default T setDistanceThreshold(Double value) {return set(DISTANCE_THRESHOLD, value);}
}
