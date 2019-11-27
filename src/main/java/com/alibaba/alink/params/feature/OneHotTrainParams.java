package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * parameters of one hot train process.
 */
public interface OneHotTrainParams<T> extends WithParams<T>,
	HasSelectedCols <T> {

	ParamInfo <Boolean> DROP_LAST = ParamInfoFactory
		.createParamInfo("dropLast", Boolean.class)
		.setDescription("drop last")
		.setHasDefaultValue(Boolean.TRUE)
		.build();
	ParamInfo <Boolean> IGNORE_NULL = ParamInfoFactory
		.createParamInfo("ignoreNull", Boolean.class)
		.setDescription("ignore null")
		.setHasDefaultValue(Boolean.FALSE)
		.build();

	default Boolean getDropLast() {
		return get(DROP_LAST);
	}

	default T setDropLast(Boolean value) {
		return set(DROP_LAST, value);
	}

	default Boolean getIgnoreNull() {
		return get(IGNORE_NULL);
	}

	default T setIgnoreNull(Boolean value) {
		return set(IGNORE_NULL, value);
	}

}
