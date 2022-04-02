package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAccessIdDefaultAsNull<T> extends WithParams <T> {
	@NameCn("accessId")
	@DescCn("accessId")
	ParamInfo <String> ACCESS_ID = ParamInfoFactory
		.createParamInfo("accessId", String.class)
		.setDescription("access id")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"accessid"})
		.build();

	default String getAccessId() {
		return get(ACCESS_ID);
	}

	default T setAccessId(String value) {
		return set(ACCESS_ID, value);
	}
}
