package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLenient<T> extends WithParams <T> {

	@NameCn("是否容错")
	@DescCn("若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常")
	ParamInfo <Boolean> LENIENT = ParamInfoFactory
		.createParamInfo("lenient", Boolean.class)
		.setDescription("lenient")
		.setHasDefaultValue(false)
		.build();

	default Boolean getLenient() {
		return get(LENIENT);
	}

	default T setLenient(Boolean value) {
		return set(LENIENT, value);
	}
}
