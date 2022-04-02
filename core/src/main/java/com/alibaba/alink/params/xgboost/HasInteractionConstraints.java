package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasInteractionConstraints<T> extends WithParams <T> {

	@NameCn("interaction constraints")
	@DescCn("interaction constraints")
	ParamInfo <String> INTERACTION_CONSTRAINTS = ParamInfoFactory
		.createParamInfo("interactionConstraints", String.class)
		.setDescription("Constraints for interaction representing permitted interactions.")
		.setHasDefaultValue(null)
		.build();

	default String getInteractionConstraints() {
		return get(INTERACTION_CONSTRAINTS);
	}

	default T setInteractionConstraints(String interactionConstraints) {
		return set(INTERACTION_CONSTRAINTS, interactionConstraints);
	}
}
