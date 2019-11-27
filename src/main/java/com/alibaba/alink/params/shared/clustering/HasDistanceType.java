package com.alibaba.alink.params.shared.clustering;

import com.alibaba.alink.operator.common.clustering.DistanceType;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.common.clustering.DistanceType;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params: Distance type for clustering, support EUCLIDEAN and COSINE.
 */
public interface HasDistanceType<T> extends WithParams<T> {
	ParamInfo <String> DISTANCE_TYPE = ParamInfoFactory
		.createParamInfo("distanceType", String.class)
		.setDescription("Distance type for clustering, support EUCLIDEAN and COSINE.")
		.setHasDefaultValue(DistanceType.EUCLIDEAN.name())
		.setAlias(new String[]{"metric"})
		.build();

	default String getDistanceType() {return get(DISTANCE_TYPE);}

	default T setDistanceType(String value) {return set(DISTANCE_TYPE, value);}
}
