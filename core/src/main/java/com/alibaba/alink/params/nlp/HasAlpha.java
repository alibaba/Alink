package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAlpha<T> extends WithParams <T> {
	@NameCn("学习率")
	@DescCn("学习率")
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("learning rate of sgd")
		.setHasDefaultValue(0.025)
		.setAlias(new String[] {"startAlpha"})
		.build();

	default Double getAlpha() {
		return get(ALPHA);
	}

	default T setAlpha(Double value) {
		return set(ALPHA, value);
	}
}
