package com.alibaba.alink.params.udf;

public interface BasePyBinaryFnParams<T> extends
	HasClassObject <T>,
	HasClassObjectType <T>,
	HasPythonVersion <T>,
	HasPythonEnvFilePath <T> {
}
