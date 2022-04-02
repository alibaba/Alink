package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSkipBlinkLineDefaultAsTrue<T> extends WithParams <T> {
	@NameCn("是否忽略空行")
	@DescCn("是否忽略空行")
	ParamInfo <Boolean> SKIP_BLANK_LINE = ParamInfoFactory
		.createParamInfo("skipBlankLine", Boolean.class)
		.setDescription("skipBlankLine")
		.setHasDefaultValue(true)
		.build();

	default Boolean getSkipBlankLine() {
		return get(SKIP_BLANK_LINE);
	}

	default T setSkipBlankLine(Boolean value) {
		return set(SKIP_BLANK_LINE, value);
	}
}
