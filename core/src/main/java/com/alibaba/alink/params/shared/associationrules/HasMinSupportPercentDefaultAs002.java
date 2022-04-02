package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinSupportPercentDefaultAs002<T> extends WithParams <T> {
	@NameCn("最小支持度占比")
	@DescCn("最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用")
	ParamInfo <Double> MIN_SUPPORT_PERCENT = ParamInfoFactory
		.createParamInfo("minSupportPercent", Double.class)
		.setDescription("Minimum support percent")
		.setHasDefaultValue(0.02)
		.build();

	default Double getMinSupportPercent() {
		return get(MIN_SUPPORT_PERCENT);
	}

	default T setMinSupportPercent(Double value) {
		return set(MIN_SUPPORT_PERCENT, value);
	}
}
