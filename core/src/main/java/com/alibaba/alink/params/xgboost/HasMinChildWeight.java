package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinChildWeight<T> extends WithParams <T> {

	@NameCn("结点的最小权重")
	@DescCn("结点的最小权重")
	ParamInfo <Double> MIN_CHILD_WEIGHT = ParamInfoFactory
		.createParamInfo("minChildWeight", Double.class)
		.setDescription("Minimum sum of instance weight (hessian) needed in a child.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getMinChildWeight() {
		return get(MIN_CHILD_WEIGHT);
	}

	default T setMinChildWeight(Double minChildWeight) {
		return set(MIN_CHILD_WEIGHT, minChildWeight);
	}
}
