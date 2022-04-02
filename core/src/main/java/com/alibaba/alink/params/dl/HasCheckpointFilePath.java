package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasCheckpointFilePath<T> extends HasRemoveCheckpointBeforeTraining <T> {
	@NameCn("保存 checkpoint 的路径")
	@DescCn("用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录")
	ParamInfo <String> CHECKPOINT_FILE_PATH = ParamInfoFactory
		.createParamInfo("checkpointFilePath", String.class)
		.setDescription("File path for saving TensorFlow checkpoints")
		.setAlias(new String[] {"estimatorModelDir"})
		.setRequired()
		.build();

	// TODO: change to return FilePath when FilePath can parse OSS URL
	default String getCheckpointFilePath() {
		return get(CHECKPOINT_FILE_PATH);
	}

	default T setCheckpointFilePath(String value) {
		return set(CHECKPOINT_FILE_PATH, value);
	}

	default T setCheckpointFilePath(FilePath filePath) {
		return set(CHECKPOINT_FILE_PATH, filePath.serialize());
	}
}
