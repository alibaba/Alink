package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDistanceThreshold<T> extends WithParams <T> {
	@NameCn("距离阈值")
	@DescCn("距离阈值")
	ParamInfo <Double> DISTANCE_THRESHOLD = ParamInfoFactory
		.createParamInfo("distanceThreshold", Double.class)
		.setDescription("distance threshold")
		.setHasDefaultValue(Double.MAX_VALUE)
		.setAlias(new String[] {"threshold"})
		.build();

	default Double getDistanceThreshold() {return get(DISTANCE_THRESHOLD);}

	default T setDistanceThreshold(Double value) {return set(DISTANCE_THRESHOLD, value);}
}
