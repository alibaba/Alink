package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDropLast<T> extends WithParams<T> {

	ParamInfo<Boolean> DROP_LAST = ParamInfoFactory
		.createParamInfo("dropLast", Boolean.class)
		.setDescription("drop last")
		.setHasDefaultValue(Boolean.TRUE)
		.build();

	default Boolean getDropLast() {
		return get(DROP_LAST);
	}

	default T setDropLast(Boolean value) {
		return set(DROP_LAST, value);
	}
}
