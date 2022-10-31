package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasHandleDuplicateFeature<T> extends WithParams <T> {
	@NameCn("重复特征处理策略")
	@DescCn("重复特征处理策略。\"first\"最终value以第一个出现的特征为准, \"last\"最终value以最后一个出现的为准， \"error\"表示抛异常")
	ParamInfo <HandleDuplicate> HANDLE_DUPLICATE_FEATURE = ParamInfoFactory
		.createParamInfo("handleDuplicate", HandleDuplicate.class)
		.setDescription("Strategy to handle duplicate token when doing prediction, one of \"first\", \"last\" or "
			+ "\"error\"")
		.setHasDefaultValue(HandleDuplicate.FIRST)
		.build();

	default HandleDuplicate getHandleDuplicate() {
		return get(HANDLE_DUPLICATE_FEATURE);
	}

	default T setHandleDuplicate(HandleDuplicate value) {
		return set(HANDLE_DUPLICATE_FEATURE, value);
	}

	default T setHandleDuplicate(String value) {
		return set(HANDLE_DUPLICATE_FEATURE, ParamUtil.searchEnum(HANDLE_DUPLICATE_FEATURE, value));
	}

	/**
	 * Strategy to handle unseen token when doing prediction.
	 */
	enum HandleDuplicate {
		FIRST,
		LAST,
		ERROR
	}
}
