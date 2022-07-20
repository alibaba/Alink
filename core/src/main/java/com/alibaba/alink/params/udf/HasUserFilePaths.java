package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.FilePath;

import java.util.Arrays;

public interface HasUserFilePaths<T> extends WithParams <T> {

	@NameCn("所有自定义脚本文件的路径")
	@DescCn("所有自定义脚本文件的路径")
	ParamInfo <String[]> USER_FILE_PATHS = ParamInfoFactory
		.createParamInfo("userFilePaths", String[].class)
		.setDescription("File paths for all user-provided files")
		.setRequired()
		.build();

	default FilePath[] getUserFilePaths() {
		String[] userFilePaths = get(USER_FILE_PATHS);
		return Arrays.stream(userFilePaths).map(FilePath::deserialize).toArray(FilePath[]::new);
	}

	default T setUserFilePaths(String... filePathStrs) {
		String[] filePathSerStrs = Arrays.stream(filePathStrs)
			.map(FilePath::new)
			.map(FilePath::serialize)
			.toArray(String[]::new);
		return set(USER_FILE_PATHS, filePathSerStrs);
	}

	default T setUserFilePaths(FilePath... filePaths) {
		String[] filePathSerStrs = Arrays.stream(filePaths).map(FilePath::serialize).toArray(String[]::new);
		return set(USER_FILE_PATHS, filePathSerStrs);
	}
}
