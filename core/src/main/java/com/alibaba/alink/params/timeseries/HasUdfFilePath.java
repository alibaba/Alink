package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

public interface HasUdfFilePath<T> extends WithParams <T> {
	@NameCn("脚本文件路径")
	@DescCn("脚本文件路径，可以是单个文件（.py）、压缩文件（.zip 或 .tar.gz）、或者目录")
	ParamInfo <String> UDF_FILE_PATH = ParamInfoFactory
		.createParamInfo("udfFilePath", String.class)
		.setDescription("UDF file path with file system.")
		.setHasDefaultValue(null)
		.build();

	default FilePath getUdfFilePath() {
		return FilePath.deserialize(get(UDF_FILE_PATH));
	}

	default T setUdfFilePath(String value) {
		return set(UDF_FILE_PATH, new FilePath(value).serialize());
	}

	default T setUdfFilePath(FilePath udfFilePath) {
		return set(UDF_FILE_PATH, udfFilePath.serialize());
	}
}
