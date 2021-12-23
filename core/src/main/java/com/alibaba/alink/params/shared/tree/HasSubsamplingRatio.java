package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSubsamplingRatio<T> extends WithParams <T> {
	/**
	 * @cn-name 每棵树的样本采样比例或采样行数
	 * @cn 每棵树的样本采样比例或采样行数，行数上限100w行
	 */
	ParamInfo <Double> SUBSAMPLING_RATIO = ParamInfoFactory
		.createParamInfo("subsamplingRatio", Double.class)
		.setDescription("Ratio of the training samples used for learning each decision tree.")
		.setHasDefaultValue(100000.0)
		.setAlias(new String[] {"factor"})
		.build();

	default Double getSubsamplingRatio() {
		return get(SUBSAMPLING_RATIO);
	}

	default T setSubsamplingRatio(Double value) {
		return set(SUBSAMPLING_RATIO, value);
	}
}
