package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSelectedColTypes<T> extends WithParams <T> {
	@NameCn("选择列类型")
	@DescCn("选择列的类型数组")
	ParamInfo <String[]> SELECTED_COL_TYPES = ParamInfoFactory
		.createParamInfo("selectedColTypes", String[].class)
		.setDescription("The types of the selected columns.")
		.setRequired()
		.build();
}
