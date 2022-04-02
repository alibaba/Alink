package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasScalePosWeight<T> extends WithParams <T> {

	@NameCn("ScalePosWeight")
	@DescCn("ScalePosWeight")
	ParamInfo <Double> SCALE_POS_WEIGHT = ParamInfoFactory
		.createParamInfo("scalePosWeight", Double.class)
		.setDescription("Control the balance of positive and negative weights, useful for unbalanced classes.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getScalePosWeight() {
		return get(SCALE_POS_WEIGHT);
	}

	default T setScalePosWeight(Double scalePosWeight) {
		return set(SCALE_POS_WEIGHT, scalePosWeight);
	}
}
