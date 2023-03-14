package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasEndPoint;
import com.alibaba.alink.params.io.shared.HasFileSystemUri;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface S3FileSystemParams<T> extends WithParams <T>,
	HasEndPoint <T>,
	HasFileSystemUri <T>,
	HasPluginVersion <T> {

	@NameCn("访问ID")
	@DescCn("访问ID")
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

	@NameCn("密钥")
	@DescCn("密钥")
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

	@NameCn("是否允许pathStyleAccess")
	@DescCn("是否允许pathStyleAccess")
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
