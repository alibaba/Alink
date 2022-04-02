package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasImputerFillValue<T> extends WithParams <T> {
	@NameCn("填充缺失值")
	@DescCn("自定义的填充值。当strategy为value时，读取fillValue的值")
	ParamInfo <String> FILL_VALUE = ParamInfoFactory
		.createParamInfo("fillValue", String.class)
		.setDescription("fill all missing values with fillValue")
		.setHasDefaultValue(null)
		.build();

	default String getFillValue() {
		return get(FILL_VALUE);
	}

	default T setFillValue(String value) {
		return set(FILL_VALUE, value);
	}
}
