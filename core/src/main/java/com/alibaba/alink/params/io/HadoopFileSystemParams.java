package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.shared_params.HasFileSystemUri;

public interface HadoopFileSystemParams<T> extends HasFileSystemUri<T> {

	ParamInfo<String> PATH_HADOOP_CONFIG = ParamInfoFactory
		.createParamInfo("pathHadoopConfig", String.class)
		.setDescription("Path of hadoop config.")
		.build();

	default String getPathHadoopConfig() {
		return get(PATH_HADOOP_CONFIG);
	}

	default T setPathHadoopConfig(String value) {
		return set(PATH_HADOOP_CONFIG, value);
	}
}
