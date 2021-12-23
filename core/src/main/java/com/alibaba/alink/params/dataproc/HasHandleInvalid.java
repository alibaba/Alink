package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasHandleInvalid<T> extends WithParams <T> {
	/**
	 * @cn-name 未知token处理策略
	 * @cn 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常
	 */
	ParamInfo <HandleInvalid> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", HandleInvalid.class)
		.setDescription("Strategy to handle unseen token when doing prediction, one of \"keep\", \"skip\" or "
			+ "\"error\"")
		.setHasDefaultValue(HandleInvalid.KEEP)
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
		 * Assign "max index" + 1.
		 */
		KEEP,
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
