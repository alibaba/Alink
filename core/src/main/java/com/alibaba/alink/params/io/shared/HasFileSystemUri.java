package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasFileSystemUri<T> extends WithParams <T> {
	@NameCn("文件系统的uri")
	@DescCn("文件系统的uri")	
	ParamInfo <String> FS_URI = ParamInfoFactory
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
