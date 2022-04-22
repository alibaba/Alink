package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasModelFilePath<T> extends WithParams <T> {

	@NameCn("模型的文件路径")
	@DescCn("模型的文件路径")
	ParamInfo <String> MODEL_FILE_PATH = ParamInfoFactory
		.createParamInfo("modelFilePath", String.class)
		.setDescription("Model file path with file system.")
		.setHasDefaultValue(null)
		.build();

	default FilePath getModelFilePath() {
		return FilePath.deserialize(get(MODEL_FILE_PATH));
	}

	default T setModelFilePath(String pathString) {
		return set(MODEL_FILE_PATH, new FilePath(pathString).serialize());
	}

	default T setModelFilePath(FilePath filePath) {
		return set(MODEL_FILE_PATH, filePath.serialize());
	}
}
