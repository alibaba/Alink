package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.io.annotations.IOType;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * IO type.
 */
public interface HasIoType<T> extends WithParams<T> {
	ParamInfo <IOType> IO_TYPE = ParamInfoFactory
		.createParamInfo("ioType", IOType.class)
		.setDescription("io type")
		.setRequired()
		.build();

	default IOType getRowDelimiter() {
		return get(IO_TYPE);
	}

	default T setRowDelimiter(IOType value) {
		return set(IO_TYPE, value);
	}
}

