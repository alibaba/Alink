package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * IO name.
 */
public interface HasIoName<T> extends WithParams <T> {

	@NameCn("IO 名称")
	@DescCn("IO 名称")
	ParamInfo <String> IO_NAME = ParamInfoFactory
		.createParamInfo("ioName", String.class)
		.setDescription("io name")
		.setRequired()
		.build();

	default String getRowDelimiter() {
		return get(IO_NAME);
	}

	default T setRowDelimiter(String value) {
		return set(IO_NAME, value);
	}
}

