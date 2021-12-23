package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumPssDefaultAsNull<T> extends WithParams <T> {

	/**
	 * @cn PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。
	 * @cn-name PS 角色数
	 */
	ParamInfo <Integer> NUM_PSS = ParamInfoFactory
		.createParamInfo("numPSs", Integer.class)
		.setDescription("number of parameter servers")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumPSs() {
		return get(NUM_PSS);
	}

	default T setNumPSs(Integer value) {
		return set(NUM_PSS, value);
	}
}
