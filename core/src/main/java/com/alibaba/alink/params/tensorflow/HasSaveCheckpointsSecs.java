package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSaveCheckpointsSecs<T> extends WithParams <T> {
	@NameCn("每隔多少秒保存 checkpoints")
	@DescCn("每隔多少秒保存 checkpoints")
	ParamInfo <Double> SAVE_CHECKPOINTS_SECS = ParamInfoFactory
		.createParamInfo("saveCheckpointsSecs", Double.class)
		.setDescription("Save checkpoints every several seconds")
		.setOptional()
		.build();

	default Double getSaveCheckpointsSecs() {
		return get(SAVE_CHECKPOINTS_SECS);
	}

	default T setSaveCheckpointsSecs(Double value) {
		return set(SAVE_CHECKPOINTS_SECS, value);
	}
}
