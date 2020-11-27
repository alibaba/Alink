package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.io.shared.HasAccessIdDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasAccessKeyDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasEndPoint;
import com.alibaba.alink.params.io.shared.HasFileSystemUri;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface OssFileSystemParams<T> extends WithParams <T>,
	HasAccessIdDefaultAsNull <T>,
	HasAccessKeyDefaultAsNull <T>,
	HasFileSystemUri <T>,
	HasEndPoint <T>,
	HasPluginVersion<T> {

	ParamInfo <String> SECURITY_TOKEN = ParamInfoFactory
		.createParamInfo("securityToken", String.class)
		.setDescription("security token")
		.setHasDefaultValue(null)
		.build();

	default String getSecurityToken() {
		return get(SECURITY_TOKEN);
	}

	default T setSecurityToken(String value) {
		return set(SECURITY_TOKEN, value);
	}
}
