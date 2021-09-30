package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputBatchAxes<T> extends WithParams <T> {

	ParamInfo <int[]> OUTPUT_BATCH_AXES = ParamInfoFactory
		.createParamInfo("outputBatchAxes", int[].class)
		.setDescription("The axis of batch dimension for each output tensor")
		.setHasDefaultValue(null)
		.build();

	default int[] getOutputBatchAxes() {
		return get(OUTPUT_BATCH_AXES);
	}

	default T setOutputBatchAxes(int[] value) {
		return set(OUTPUT_BATCH_AXES, value);
	}
}
