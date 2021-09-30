package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.dl.ExternalFilesConfig;

public interface HasUserFiles<T> extends WithParams <T> {
	ParamInfo <String> USER_FILES = ParamInfoFactory
		.createParamInfo("userFiles", String.class)
		.setAlias(new String[] {"userFile", "scriptFiles"})
		.setDescription("userFiles")
		.build();

	default ExternalFilesConfig getUserFiles() {
		return ExternalFilesConfig.fromJson(get(USER_FILES));
	}

	default T setUserFiles(String value) {
		return set(USER_FILES, value);
	}

	default T setUserFiles(String[] values) {
		return set(USER_FILES, new ExternalFilesConfig().addFilePaths(values).toJson());
	}

	default T setUserFiles(ExternalFilesConfig externalFilesConfig) {
		return set(USER_FILES, externalFilesConfig.toJson());
	}
}
