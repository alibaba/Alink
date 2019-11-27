package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * positive label value with string format.
 */
public interface HasPositiveLabelValueString<T> extends WithParams<T> {
	ParamInfo <String> POS_LABEL_VAL_STR = ParamInfoFactory
		.createParamInfo("positiveLabelValueString", String.class)
		.setDescription("positive label value with string format.")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"predPositiveLabelValueString", "positiveValue"})
		.build();

	default String getPositiveLabelValueString() {
		return get(POS_LABEL_VAL_STR);
	}

	default T setPositiveLabelValueString(String value) {
		return set(POS_LABEL_VAL_STR, value);
	}
}
