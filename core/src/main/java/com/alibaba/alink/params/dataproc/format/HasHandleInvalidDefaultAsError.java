package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasHandleInvalidDefaultAsError<T> extends WithParams <T> {
	@NameCn("解析异常处理策略")
	@DescCn("解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL）")
	ParamInfo <HandleInvalid> HANDLE_INVALID = ParamInfoFactory
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
