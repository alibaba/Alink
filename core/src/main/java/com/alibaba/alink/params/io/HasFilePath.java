package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasFilePath<T> extends WithParams <T> {
	@NameCn("文件路径")
	@DescCn("文件路径")
	ParamInfo <String> FILE_PATH = ParamInfoFactory
		.createParamInfo("filePath", String.class)
		.setDescription("File path with file system.")
		.setRequired()
		.build();

	default FilePath getFilePath() {
		return FilePath.deserialize(get(FILE_PATH));
	}

	default T setFilePath(String value) {
		return set(FILE_PATH, new FilePath(value).serialize());
	}

	default T setFilePath(FilePath filePath) {
		return set(FILE_PATH, filePath.serialize());
	}
}
