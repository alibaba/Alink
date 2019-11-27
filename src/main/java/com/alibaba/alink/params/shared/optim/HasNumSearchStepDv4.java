package com.alibaba.alink.params.shared.optim;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Line search parameter, which define the search value num of one step.
 */
public interface HasNumSearchStepDv4<T> extends WithParams<T> {

	ParamInfo <Integer> NUM_SEARCH_STEP = ParamInfoFactory
		.createParamInfo("numSearchStep", Integer.class)
		.setDescription("num search step")
		.setHasDefaultValue(4)
		.build();

	default Integer getNumSearchStep() {
		return get(NUM_SEARCH_STEP);
	}

	default T setNumSearchStep(Integer value) {
		return set(NUM_SEARCH_STEP, value);
	}
}
