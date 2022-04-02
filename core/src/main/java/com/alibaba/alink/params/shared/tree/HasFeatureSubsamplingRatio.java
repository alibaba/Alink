package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasFeatureSubsamplingRatio<T> extends WithParams <T> {
	@NameCn("每棵树特征采样的比例")
	@DescCn("每棵树特征采样的比例，范围为(0, 1]。")
	ParamInfo <Double> FEATURE_SUBSAMPLING_RATIO = ParamInfoFactory
		.createParamInfo("featureSubsamplingRatio", Double.class)
		.setDescription("Ratio of the features used in each tree, in range (0, 1].")
		.setHasDefaultValue(0.2)
		.setAlias(new String[] {"factor"})
		.build();

	default Double getFeatureSubsamplingRatio() {
		return get(FEATURE_SUBSAMPLING_RATIO);
	}

	default T setFeatureSubsamplingRatio(Double value) {
		return set(FEATURE_SUBSAMPLING_RATIO, value);
	}
}
