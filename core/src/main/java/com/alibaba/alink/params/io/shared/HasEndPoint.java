package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasEndPoint<T> extends WithParams <T> {
	@NameCn("endpoint")
	@DescCn("endpoint")
	ParamInfo <String> END_POINT = ParamInfoFactory
		.createParamInfo("endPoint", String.class)
		.setDescription("end point")
		.setRequired()
		.setAlias(new String[] {"endpoint", "end_point"})
		.build();

	default String getEndPoint() {
		return get(END_POINT);
	}

	default T setEndPoint(String value) {
		return set(END_POINT, value);
	}
}
