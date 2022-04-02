package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDetectLast<T> extends WithParams <T> {

	ParamInfo <Boolean> DETECT_LAST = ParamInfoFactory
		.createParamInfo("detectLast", Boolean.class)
		.setDescription("Only detect the last row.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getDetectLast() {
		return get(DETECT_LAST);
	}

	default T setDetectLast(Boolean val) {
		return set(DETECT_LAST, val);
	}
}
