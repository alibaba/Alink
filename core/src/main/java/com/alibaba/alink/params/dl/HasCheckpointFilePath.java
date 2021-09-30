package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.params.dl.HasRemoveCheckpointBeforeTraining;

public interface HasCheckpointFilePath<T> extends HasRemoveCheckpointBeforeTraining <T> {

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
