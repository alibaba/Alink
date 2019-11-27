package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasItemsCol<T> extends WithParams<T> {
	ParamInfo <String> ITEMS_COL = ParamInfoFactory
		.createParamInfo("itemsCol", String.class)
		.setAlias(new String[]{"itemsColName", "itemColName"})
		.setDescription("Column name of transaction items")
		.setRequired()
		.build();

	default String getItemsCol() {
		return get(ITEMS_COL);
	}

	default T setItemsCol(String value) {
		return set(ITEMS_COL, value);
	}
}
