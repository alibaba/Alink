package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: Projection length, used in bucket random projection LSH.
 */
public interface HasProjectionWidth<T> extends WithParams <T> {
	/**
	 * @cn-name 桶的宽度
	 * @cn 桶的宽度
	 */
	ParamInfo <Double> PROJECTION_WIDTH = ParamInfoFactory
		.createParamInfo("projectionWidth", Double.class)
		.setDescription("Bucket length, used in bucket random projection LSH.")
		.setHasDefaultValue(1.0)
		.setAlias(new String[] {"bucketLength"})
		.build();

	default Double getProjectionWidth() {
		return get(PROJECTION_WIDTH);
	}

	default T setProjectionWidth(Double value) {
		return set(PROJECTION_WIDTH, value);
	}
}
