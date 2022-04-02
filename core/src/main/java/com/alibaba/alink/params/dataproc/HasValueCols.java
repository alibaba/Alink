package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasValueCols<T> extends WithParams <T> {
	@NameCn("多数值列")
	@DescCn("多数值列")
	ParamInfo <String[]> VALUE_COLS = ParamInfoFactory
		.createParamInfo("valueCols", String[].class)
		.setDescription("value colume names")
		.setHasDefaultValue(null)
		.build();

	default String[] getValueCols() {return get(VALUE_COLS);}

	default T setValueCols(String... value) {return set(VALUE_COLS, value);}
}
