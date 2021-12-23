package com.alibaba.alink.params.tensorflow.kerasequential;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOptimizer<T> extends WithParams <T> {
	/**
	 * @cn-name 优化器
	 * @cn 优化器，使用 Python 语法，例如 "Adam(learning_rate=0.1)"
	 */
	ParamInfo <String> OPTIMIZER = ParamInfoFactory
		.createParamInfo("optimizer", String.class)
		.setDescription("Optimizer, in Python language, "
			+ "for example \"Adam(learning_rate=0.1)\"")
		.setHasDefaultValue("Adam()")
		.build();

	default String getOptimizer() {
		return get(OPTIMIZER);
	}

	default T setOptimizer(String layers) {
		return set(OPTIMIZER, layers);
	}
}
