package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasGroupCol<T> extends WithParams <T> {
	@NameCn("分组列")
	@DescCn("分组单列名，必选")
	ParamInfo <String> GROUP_COL = ParamInfoFactory
		.createParamInfo("groupCol", String.class)
		.setDescription("Name of a grouping column")
		.setAlias(new String[] {"groupColName", "groupIdCol", "groupIdColName"})
		.setRequired()
		.build();

	default String getGroupCol() {
		return get(GROUP_COL);
	}

	default T setGroupCol(String colName) {
		return set(GROUP_COL, colName);
	}

	@Deprecated
	default T setGroupIdCol(String colName) {
		return set(GROUP_COL, colName);
	}
}
