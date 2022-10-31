package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSaveCheckpointsEpochs<T> extends WithParams <T> {
	@NameCn("每隔多少 epochs 保存 checkpoints")
	@DescCn("每隔多少 epochs 保存 checkpoints")
	ParamInfo <Double> SAVE_CHECKPOINTS_EPOCHS = ParamInfoFactory
		.createParamInfo("saveCheckpointsEpochs", Double.class)
		.setDescription("Save checkpoints every several epochs")
		.setHasDefaultValue(1.)
		.build();

	default Double getSaveCheckpointsEpochs() {
		return get(SAVE_CHECKPOINTS_EPOCHS);
	}

	default T setSaveCheckpointsEpochs(Double value) {
		return set(SAVE_CHECKPOINTS_EPOCHS, value);
	}
}
