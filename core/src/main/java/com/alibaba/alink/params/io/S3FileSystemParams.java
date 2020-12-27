package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasEndPoint;
import com.alibaba.alink.params.io.shared.HasFileSystemUri;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface S3FileSystemParams<T> extends WithParams <T>,
	HasEndPoint <T>,
	HasFileSystemUri <T>,
	HasPluginVersion <T> {

	ParamInfo <String> ACCESS_KEY = ParamInfoFactory
		.createParamInfo("accessKey", String.class)
		.setDescription("access key")
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default String getAccessKey() {
		return get(ACCESS_KEY);
	}

	default T setAccessKey(String value) {
		return set(ACCESS_KEY, value);
	}

	ParamInfo <String> SECRET_KEY = ParamInfoFactory
		.createParamInfo("secretKey", String.class)
		.setDescription("secret key")
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default String getSecretKey() {
		return get(SECRET_KEY);
	}

	default T setSecretKey(String value) {
		return set(SECRET_KEY, value);
	}

	ParamInfo <Boolean> PATH_STYLE_ACCESS = ParamInfoFactory
		.createParamInfo("pathStyleAccess", Boolean.class)
		.setDescription("path style access")
		.setHasDefaultValue(true)
		.build();

	default Boolean getPathStyleAccess() {
		return get(PATH_STYLE_ACCESS);
	}

	default T setPathStyleAccess(Boolean value) {
		return set(PATH_STYLE_ACCESS, value);
	}
}
