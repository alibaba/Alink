package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasFieldDelimiterDefaultAsComma<T> extends WithParams <T> {
	@NameCn("字段分隔符")
	@DescCn("字段分隔符")
	ParamInfo <String> FIELD_DELIMITER = ParamInfoFactory
		.createParamInfo("fieldDelimiter", String.class)
		.setDescription("Field delimiter")
		.setHasDefaultValue(",")
		.build();

	default String getFieldDelimiter() {
		return get(FIELD_DELIMITER);
	}

	default T setFieldDelimiter(String value) {
		return set(FIELD_DELIMITER, value);
	}
}
