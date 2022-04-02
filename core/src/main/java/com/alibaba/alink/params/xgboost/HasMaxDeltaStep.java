package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxDeltaStep<T> extends WithParams <T> {

	@NameCn("Delta step")
	@DescCn("Delta step")
	ParamInfo <Double> MAX_DELTA_STEP = ParamInfoFactory
		.createParamInfo("maxDeltaStep", Double.class)
		.setDescription("Maximum delta step we allow each leaf output to be.")
		.setHasDefaultValue(0.0)
		.build();

	default Double getMaxDeltaStep() {
		return get(MAX_DELTA_STEP);
	}

	default T setMaxDeltaStep(Double maxDepth) {
		return set(MAX_DELTA_STEP, maxDepth);
	}
}
