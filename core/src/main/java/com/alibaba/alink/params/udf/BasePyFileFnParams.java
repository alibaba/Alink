package com.alibaba.alink.params.udf;

public interface BasePyFileFnParams<T> extends
	HasClassName <T>,
	HasUserFilePaths <T>,
	HasPythonVersion <T>,
	HasPythonEnvFilePath <T> {
}
