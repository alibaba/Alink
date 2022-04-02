package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

/**
 * method to deal with invalid situation.
 */
public interface HasHandleInvalid<T> extends WithParams <T> {
	@NameCn("处理无效值的方法")
	@DescCn("处理无效值的方法，可取 error, skip")
	ParamInfo <HandleInvalidMethod> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalidMethod", HandleInvalidMethod.class)
		.setDescription("the handle method of invalid value. include： error, skip")
		.setAlias(new String[] {"handleInvalid"})
		.setHasDefaultValue(HandleInvalidMethod.ERROR)
		.build();

	default HandleInvalidMethod getHandleInvalidMethod() {
		return get(HANDLE_INVALID);
	}

	default T setHandleInvalidMethod(HandleInvalidMethod value) {
		return set(HANDLE_INVALID, value);
	}

	default T setHandleInvalidMethod(String value) {
		return set(HANDLE_INVALID, ParamUtil.searchEnum(HANDLE_INVALID, value));
	}

	enum HandleInvalidMethod {
		/**
		 * Raise exception.
		 */
		ERROR,
		/**
		 * Filter out rows with invalid data.
		 */
		SKIP
	}
}