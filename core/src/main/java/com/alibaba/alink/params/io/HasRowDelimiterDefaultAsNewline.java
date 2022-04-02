package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasRowDelimiterDefaultAsNewline<T> extends WithParams <T> {
	@NameCn("行分隔符")
	@DescCn("行分隔符")
	ParamInfo <String> ROW_DELIMITER = ParamInfoFactory
		.createParamInfo("rowDelimiter", String.class)
		.setDescription("Row delimiter")
		.setHasDefaultValue("\n")
		.build();

	default String getRowDelimiter() {
		return get(ROW_DELIMITER);
	}

	default T setRowDelimiter(String value) {
		return set(ROW_DELIMITER, value);
	}
}
