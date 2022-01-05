package com.alibaba.alink.params.image;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRelativeFilePathCol<T> extends WithParams <T> {

	/**
	 * @cn-name 文件路径列
	 * @cn 文件路径列
	 */
	ParamInfo <String> RELATIVE_FILE_PATH_COL = ParamInfoFactory
		.createParamInfo("relativeFilePathCol", String.class)
		.setDescription("relative file path column")
		.setRequired()
		.build();

	default String getRelativeFilePathCol() {
		return get(RELATIVE_FILE_PATH_COL);
	}

	default T setRelativeFilePathCol(String value) {
		return set(RELATIVE_FILE_PATH_COL, value);
	}
}
