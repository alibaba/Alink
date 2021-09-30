package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputSignatureDefs<T> extends WithParams <T> {


	ParamInfo <String[]> OUTPUT_SIGNATURE_DEFS = ParamInfoFactory
		.createParamInfo("outputSignatureDefs", String[].class)
		.setDescription("output signature defs")
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputSignatureDefs() {
		return get(OUTPUT_SIGNATURE_DEFS);
	}

	default T setOutputSignatureDefs(String[] value) {
		return set(OUTPUT_SIGNATURE_DEFS, value);
	}
}
