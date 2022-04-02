package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasGroupColDefaultAsNull<T> extends WithParams <T> {
	@NameCn("分组单列名")
	@DescCn("分组单列名，可选")
	ParamInfo <String> GROUP_COL = ParamInfoFactory
		.createParamInfo("groupCol", String.class)
		.setDescription("Name of a grouping column")
		.setAlias(new String[] {"groupColName"})
		.setHasDefaultValue(null)
		.build();

	default String getGroupCol() {
		return get(GROUP_COL);
	}

	default T setGroupCol(String colName) {
		return set(GROUP_COL, colName);
	}
}
