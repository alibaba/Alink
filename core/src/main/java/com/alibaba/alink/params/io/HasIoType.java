package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.io.annotations.IOType;

/**
 * IO type.
 */
public interface HasIoType<T> extends WithParams <T> {
	/**
	 * @cn IO类型.
	 */
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

