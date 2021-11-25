package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPythonEnv<T> extends WithParams <T> {
	/**
	 * @cn-name Python 环境路径
	 * @cn Python 环境路径，一般情况下不需要填写。
	 * 如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；
	 * 如果是目录，那么只能使用本地路径，即 file://。
	 */
	ParamInfo <String> PYTHON_ENV = ParamInfoFactory
		.createParamInfo("pythonEnv", String.class)
		.setDescription("Path for Python environment. No need to set this parameter in most cases.")
		.setHasDefaultValue("")
		.build();

	default String getPythonEnv() {
		return get(PYTHON_ENV);
	}

	default T setPythonEnv(String value) {
		return set(PYTHON_ENV, value);
	}
}
