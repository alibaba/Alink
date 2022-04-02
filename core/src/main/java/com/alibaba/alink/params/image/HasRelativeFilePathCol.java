package com.alibaba.alink.params.image;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasRelativeFilePathCol<T> extends WithParams <T> {

	@NameCn("文件路径列")
	@DescCn("文件路径列")
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
