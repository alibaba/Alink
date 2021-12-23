package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputBatchAxes<T> extends WithParams <T> {
	/**
	 * @cn-name 输出 Tensor 中代表 Batch 维度的 axis
	 * @cn 输出 Tensor 中代表 Batch 维度的 axis，默认均为 0
	 */
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
