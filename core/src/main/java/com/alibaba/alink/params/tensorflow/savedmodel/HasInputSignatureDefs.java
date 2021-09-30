package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasInputSignatureDefs<T> extends WithParams <T> {

	ParamInfo <String[]> INPUT_SIGNATURE_DEFS = ParamInfoFactory
		.createParamInfo("inputSignatureDefs", String[].class)
		.setDescription("input signature defs corresponding to selected columns in the SavedModel")
		.setHasDefaultValue(null)
		.build();

	default String[] getInputSignatureDefs() {
		return get(INPUT_SIGNATURE_DEFS);
	}

	default T setInputSignatureDefs(String[] value) {
		return set(INPUT_SIGNATURE_DEFS, value);
	}
}
