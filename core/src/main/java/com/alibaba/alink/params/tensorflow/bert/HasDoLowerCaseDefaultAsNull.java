package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDoLowerCaseDefaultAsNull<T> extends WithParams <T> {
	@NameCn("是否将文本转换为小写")
	@DescCn("是否将文本转换为小写，默认根据模型自动决定")
	ParamInfo <Boolean> DO_LOWER_CASE = ParamInfoFactory
		.createParamInfo("doLowerCase", Boolean.class)
		.setDescription("Whether to lower case the input text. Derived from model config by default.")
		.setHasDefaultValue(null)
		.build();

	default Boolean getDoLowerCase() {
		return get(DO_LOWER_CASE);
	}

	default T setDoLowerCase(Boolean value) {
		return set(DO_LOWER_CASE, value);
	}
}
