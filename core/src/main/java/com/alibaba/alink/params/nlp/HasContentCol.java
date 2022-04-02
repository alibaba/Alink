package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasContentCol<T> extends WithParams <T> {
	@NameCn("文本列")
	@DescCn("文本列名")
	ParamInfo <String> CONTENT_COL = ParamInfoFactory
		.createParamInfo("contentCol", String.class)
		.setDescription("Name of the column indicating document content")
		.setRequired()
		.setAlias(new String[] {"sentenceColName", "contentColName"})
		.build();

	default String getContentCol() {
		return get(CONTENT_COL);
	}

	default T setContentCol(String value) {
		return set(CONTENT_COL, value);
	}
}
