package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

public interface DCTParams<T>
	extends SISOMapperParams<T> {

	ParamInfo <Boolean> INVERSE = ParamInfoFactory
		.createParamInfo("inverse", Boolean.class)
		.setDescription(
			"If true, perform inverse transformation(e.g. inverse DCT/inverse FFT). Otherwise perform (forward) "
				+ "transformation. Default: false ")
		.setHasDefaultValue(false)
		.build();

	default Boolean getInverse() {
		return get(INVERSE);
	}

	default T setInverse(Boolean value) {
		return set(INVERSE, value);
	}
}
