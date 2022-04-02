package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAlpha<T> extends WithParams <T> {

	@NameCn("L1 正则项")
	@DescCn("L1 正则项")
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("L1 regularization term on weights.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getAlpha() {
		return get(ALPHA);
	}

	default T setAlpha(Double alpha) {
		return set(ALPHA, alpha);
	}
}
