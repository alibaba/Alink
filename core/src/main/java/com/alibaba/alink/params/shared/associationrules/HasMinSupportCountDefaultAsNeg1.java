package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinSupportCountDefaultAsNeg1<T> extends WithParams<T> {
	ParamInfo <Integer> MIN_SUPPORT_COUNT = ParamInfoFactory
		.createParamInfo("minSupportCount", Integer.class)
		.setDescription("Minimum support count")
		.setHasDefaultValue(-1)
		.build();

	default Integer getMinSupportCount() {
		return get(MIN_SUPPORT_COUNT);
	}

	default T setMinSupportCount(Integer value) {
		return set(MIN_SUPPORT_COUNT, value);
	}
}
