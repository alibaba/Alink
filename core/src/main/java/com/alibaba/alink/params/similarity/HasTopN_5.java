package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * TopN.
 * @param <T>
 */
public interface HasTopN_5<T> extends
		WithParams<T> {
	ParamInfo <Integer> TOP_N = ParamInfoFactory
			.createParamInfo("topN", Integer.class)
			.setDescription("top n")
			.setHasDefaultValue(5)
			.build();

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}
}
