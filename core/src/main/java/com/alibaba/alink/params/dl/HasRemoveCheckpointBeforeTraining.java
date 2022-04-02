package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasRemoveCheckpointBeforeTraining<T> extends WithParams <T> {
	@NameCn("是否在训练前移除 checkpoint 相关文件")
	@DescCn("是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件")
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
