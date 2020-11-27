package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNewtonStep<T> extends WithParams <T> {
	ParamInfo <Boolean> NEWTON_STEP = ParamInfoFactory
		.createParamInfo("newtonStep", Boolean.class)
		.setDescription("If open the newton step in gbdt.")
		.setHasDefaultValue(true)
		.build();

	default Boolean getNewtonStep() {
		return get(NEWTON_STEP);
	}

	default T setNewtonStep(Boolean value) {
		return set(NEWTON_STEP, value);
	}
}
