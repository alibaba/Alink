package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLeftOpen<T> extends WithParams <T> {
	@NameCn("是否左开右闭")
	@DescCn("左开右闭为true，左闭右开为false")
	ParamInfo <Boolean> LEFT_OPEN = ParamInfoFactory
		.createParamInfo("leftOpen", Boolean.class)
		.setDescription("indicating if the intervals should be opened on the left.")
		.setHasDefaultValue(true)
		.build();

	default Boolean getLeftOpen() {
		return get(LEFT_OPEN);
	}

	default T setLeftOpen(Boolean value) {
		return set(LEFT_OPEN, value);
	}
}
