package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIfGARCH11<T> extends WithParams <T> {


	ParamInfo <Boolean> IF_GARCH11 = ParamInfoFactory
		.createParamInfo("ifGARCH11", Boolean.class)
		.setDescription("ifGARCH11")
		.setHasDefaultValue(true)
		.build();

	default Boolean getIfGARCH11() {
		return get(IF_GARCH11);
	}

	default T setIfGARCH11(Boolean value) {
		return set(IF_GARCH11, value);
	}
}
