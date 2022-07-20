package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasPythonEnvFilePath<T> extends WithParams <T> {
	@NameCn("Python 环境路径")
	@DescCn(
		"Python 环境路径，一般情况下不需要填写。如果该路径是一个压缩文件，要求解压后是以下情况之一：1. 包含多个目录和文件，是一个完整的 Python 环境，其中 bin/python3 是 Python 的可执行文件；2. 包含一个目录，且目录名与压缩文件主文件名一致，该目录内是一个完整的 Python 环境。如果该路径是一个目录，那么只能使用本地路径。")
	ParamInfo <String> PYTHON_ENV_FILE_PATH = ParamInfoFactory
		.createParamInfo("pythonEnvFilePath", String.class)
		.setDescription("Path for Python environment. No need to set this parameter in most cases.")
		.build();

	default FilePath getPythonEnvFilePath() {
		return FilePath.deserialize(get(PYTHON_ENV_FILE_PATH));
	}

	default T setPythonEnvFilePath(String value) {
		return set(PYTHON_ENV_FILE_PATH, new FilePath(value).serialize());
	}

	default T setPythonEnvFilePath(FilePath filePath) {
		return set(PYTHON_ENV_FILE_PATH, filePath.serialize());
	}
}
