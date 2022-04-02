package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasGroupColsDefaultAsNull<T> extends WithParams <T> {
	@NameCn("分组列名数组")
	@DescCn("分组列名，多列，可选，默认不选")
	ParamInfo <String[]> GROUP_COLS = ParamInfoFactory
		.createParamInfo("groupCols", String[].class)
		.setDescription("group col names")
		.setAlias(new String[] {"groupColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getGroupCols() {return get(GROUP_COLS);}

	default T setGroupCols(String... colNames) {return set(GROUP_COLS, colNames);}
}
