package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRemoveCheckpointBeforeTraining<T> extends WithParams <T> {
	/**
	 * @cn-name 是否在训练前移除 checkpoint 相关文件
	 * @cn 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件
	 */
	ParamInfo <Boolean> REMOVE_CHECKPOINT_BEFORE_TRAINING = ParamInfoFactory
		.createParamInfo("removeCheckpointBeforeTraining", Boolean.class)
		.setDescription("Whether to remove checkpoint-related files before training to start over")
		.setHasDefaultValue(null)
		.build();

	default Boolean getRemoveCheckpointBeforeTraining() {
		return get(REMOVE_CHECKPOINT_BEFORE_TRAINING);
	}

	default T setRemoveCheckpointBeforeTraining(Boolean value) {
		return set(REMOVE_CHECKPOINT_BEFORE_TRAINING, value);
	}
}
