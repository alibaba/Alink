package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTimeCol_null<T> extends WithParams <T> {

	@NameCn("时间列")
	@DescCn("时间列。如果用户输入时间列，则以此作为数据的时间；否则按照process time作为数据的时间。")
	ParamInfo <String> TIME_COL = ParamInfoFactory
		.createParamInfo("timeCol", String.class)
		.setAlias(new String[] {"timeColName"})
		.setDescription("time col name")
		.setHasDefaultValue(null)
		.build();

	default String getTimeCol() {
		return get(TIME_COL);
	}

	default T setTimeCol(String value) {
		return set(TIME_COL, value);
	}

}
