package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasRootFilePath<T> extends WithParams <T> {
	@NameCn("文件路径")
	@DescCn("文件路径")
	ParamInfo <String> ROOT_FILE_PATH = ParamInfoFactory
		.createParamInfo("rootFilePath", String.class)
		.setDescription("Root file path with file system.")
		.setRequired()
		.build();

	default FilePath getRootFilePath() {
		return FilePath.deserialize(get(ROOT_FILE_PATH));
	}

	default T setRootFilePath(String value) {
		return set(ROOT_FILE_PATH, new FilePath(value).serialize());
	}

	default T setRootFilePath(FilePath filePath) {
		return set(ROOT_FILE_PATH, filePath.serialize());
	}
}
