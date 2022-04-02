package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDocIdCol<T> extends WithParams <T> {
	@NameCn("文档ID列")
	@DescCn("文档ID列名")
	ParamInfo <String> DOC_ID_COL = ParamInfoFactory
		.createParamInfo("docIdCol", String.class)
		.setDescription("Name of the column indicating document ID.")
		.setAlias(new String[] {"docIdColName"})
		.setRequired()
		.build();

	default String getDocIdCol() {
		return get(DOC_ID_COL);
	}

	default T setDocIdCol(String value) {
		return set(DOC_ID_COL, value);
	}
}
