package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLambda<T> extends WithParams <T> {

	@NameCn("L2 正则项")
	@DescCn("L2 正则项")
	ParamInfo <Double> LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("L2 regularization term on weights.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getLambda() {
		return get(LAMBDA);
	}

	default T setLambda(Double lambda) {
		return set(LAMBDA, lambda);
	}
}
