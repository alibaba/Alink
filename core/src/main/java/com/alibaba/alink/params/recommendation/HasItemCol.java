package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasItemCol<T> extends WithParams <T> {
	@NameCn("Item列列名")
	@DescCn("Item列列名")
	ParamInfo <String> ITEM_COL = ParamInfoFactory
		.createParamInfo("itemCol", String.class)
		.setAlias(new String[] {"itemColName"})
		.setDescription("Item column name")
		.setRequired()
		.build();

	default String getItemCol() {
		return get(ITEM_COL);
	}

	default T setItemCol(String value) {
		return set(ITEM_COL, value);
	}
}
