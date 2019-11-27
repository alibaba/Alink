package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Params for binarizer.
 */
public interface BinarizerParams<T> extends
	SISOMapperParams<T> {

	ParamInfo <Double> THRESHOLD = ParamInfoFactory
		.createParamInfo("threshold", Double.class)
		.setDescription(
			"Binarization threshold, when number is greater than or equal to threshold, it will be set 1.0, else 0.0.")
		.setHasDefaultValue(0.0)
		.build();

	default Double getThreshold() {return get(THRESHOLD);}

	default T setThreshold(Double value) {return set(THRESHOLD, value);}

}
