package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasIdCol<T> extends WithParams <T> {
	@NameCn("id列名")
	@DescCn("id列名")
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("id colname")
		.setRequired()
		.setAlias(new String[] {"idColName"})
		.build();

	default String getIdCol() {
		return get(ID_COL);
	}

	default T setIdCol(String value) {
		return set(ID_COL, value);
	}

}
