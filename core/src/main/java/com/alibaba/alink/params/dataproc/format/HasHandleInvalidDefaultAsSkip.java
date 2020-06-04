package com.alibaba.alink.params.dataproc.format;

import com.alibaba.alink.params.ParamUtil;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasHandleInvalidDefaultAsSkip<T> extends WithParams <T> {
	/**
	 * @cn-name 解析异常处理策略
	 * @cn 解析异常处理策略
	 */
	ParamInfo<HandleInvalid> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", HandleInvalid.class)
		.setDescription("Strategy to handle unseen token")
		.setHasDefaultValue(HandleInvalid.ERROR)
		.build();

	default HandleInvalid getHandleInvalid() {
		return get(HANDLE_INVALID);
	}

	default T setHandleInvalid(HandleInvalid value) {
		return set(HANDLE_INVALID, value);
	}

	default T setHandleInvalid(String value) {
		return set(HANDLE_INVALID, ParamUtil.searchEnum(HANDLE_INVALID, value));
	}

	/**
	 * Strategy to handle unseen token when doing prediction.
	 */
	enum HandleInvalid {
		/**
		 * Raise exception.
		 */
		ERROR,
		/**
		 * Pad with null.
		 */
		SKIP
	}
}
