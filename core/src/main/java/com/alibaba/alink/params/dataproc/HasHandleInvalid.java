package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasHandleInvalid<T> extends WithParams<T> {

	ParamInfo<String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", String.class)
		.setDescription("Strategy to handle unseen token when doing prediction, one of \"keep\", \"skip\" or "
			+ "\"error\"")
		.setHasDefaultValue("keep")
		.build();

	default String getHandleInvalid() {
		return get(HANDLE_INVALID);
	}

	default T setHandleInvalid(String value) {
		return set(HANDLE_INVALID, value);
	}
}
