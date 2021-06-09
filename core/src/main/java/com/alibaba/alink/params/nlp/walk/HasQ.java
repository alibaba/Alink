package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasQ<T> extends WithParams <T> {
	ParamInfo <Double> Q = ParamInfoFactory
		.createParamInfo("q", Double.class)
		.setDescription("In-out parameter, q. Parameter q allows the search to differentiate\n" +
			"between “inward” and “outward” nodes")
		.setHasDefaultValue(1.0)
		.build();

	default Double getQ() {
		return get(Q);
	}

	default T setQ(Double value) {
		return set(Q, value);
	}
}
