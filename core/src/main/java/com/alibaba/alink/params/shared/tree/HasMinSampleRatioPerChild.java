package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinSampleRatioPerChild<T> extends WithParams<T> {
	ParamInfo <Double> MIN_SAMPLE_RATIO_PERCHILD = ParamInfoFactory
		.createParamInfo("minSampleRatioPerChild", Double.class)
		.setDescription("Minimal value of: (num of samples in child)/(num of samples in its parent).")
		.setHasDefaultValue(0.0)
		.setAlias(new String[] {"minPercent"})
		.build();

	default Double getMinSampleRatioPerChild() {
		return get(MIN_SAMPLE_RATIO_PERCHILD);
	}

	default T setMinSampleRatioPerChild(Double value) {
		return set(MIN_SAMPLE_RATIO_PERCHILD, value);
	}
}
