package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSaveCheckpointsSteps<T> extends WithParams <T> {
	@NameCn("每隔多少 Steps 保存 checkpoints")
	@DescCn("每隔多少 Steps 保存 checkpoints")
	ParamInfo <Integer> SAVE_CHECKPOINTS_STEPS = ParamInfoFactory
		.createParamInfo("saveCheckpointsSteps", Integer.class)
		.setDescription("Save checkpoints every several Steps")
		.setHasDefaultValue(60)
		.build();

	default Integer getSaveCheckpointsSteps() {
		return get(SAVE_CHECKPOINTS_STEPS);
	}

	default T setSaveCheckpointsSteps(Integer value) {
		return set(SAVE_CHECKPOINTS_STEPS, value);
	}
}
