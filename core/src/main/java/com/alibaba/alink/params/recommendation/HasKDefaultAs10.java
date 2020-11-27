package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params: Number of the recommended top objects.
 */
public interface HasKDefaultAs10<T> extends WithParams <T> {
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("Number of the recommended top objects.")
		.setAlias(new String[] {"topk", "topK"})
		.setHasDefaultValue(10)
		.build();

	default Integer getK() {
		return get(K);
	}

	default T setK(Integer value) {
		return set(K, value);
	}
}
