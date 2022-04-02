package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Param: Projection length, used in bucket random projection LSH.
 */
public interface HasProjectionWidth<T> extends WithParams <T> {
	@NameCn("桶的宽度")
	@DescCn("桶的宽度")
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
