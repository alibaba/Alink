package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTextCol<T> extends WithParams <T> {
	@NameCn("文本列名称")
	@DescCn("文本列名称")
	ParamInfo <String> TEXT_COL = ParamInfoFactory
		.createParamInfo("textCol", String.class)
		.setDescription("Text Column Name")
		.setHasDefaultValue("text")
		.build();

	default String getTextCol() {
		return get(TEXT_COL);
	}

	default T setTextCol(String textCol) {
		return set(TEXT_COL, textCol);
	}
}
