package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMonotoneConstraints<T> extends WithParams <T> {

	@NameCn("monotone constraints")
	@DescCn("monotone constraints")
	ParamInfo <String> MONOTONE_CONSTRAINTS = ParamInfoFactory
		.createParamInfo("monotoneConstraints", String.class)
		.setDescription("Constraint of variable monotonicity.")
		.setHasDefaultValue(null)
		.build();

	default String getMonotoneConstraints() {
		return get(MONOTONE_CONSTRAINTS);
	}

	default T setMonotoneConstraints(String monotoneConstraints) {
		return set(MONOTONE_CONSTRAINTS, monotoneConstraints);
	}
}
