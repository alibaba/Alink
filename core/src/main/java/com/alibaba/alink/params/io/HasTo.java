package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTo<T> extends WithParams <T> {

	@NameCn("截止")
	@DescCn("截止")
	ParamInfo <Long> TO = ParamInfoFactory
		.createParamInfo("to", Long.class)
		.setRequired()
		.build();

	default T setTo(long value) {
		set(TO, value);
		return (T) this;
	}

	default Long getTo() {
		return get(TO);
	}
}
