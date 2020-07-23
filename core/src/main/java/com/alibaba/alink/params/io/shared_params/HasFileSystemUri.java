package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFileSystemUri<T> extends WithParams<T> {

	ParamInfo<String> FS_URI = ParamInfoFactory
		.createParamInfo("fsUri", String.class)
		.setDescription("Uri of the file system.")
		.setHasDefaultValue(null)
		.build();

	default String getFSUri() {
		return get(FS_URI);
	}

	default T setFSUri(String value) {
		return set(FS_URI, value);
	}
}
