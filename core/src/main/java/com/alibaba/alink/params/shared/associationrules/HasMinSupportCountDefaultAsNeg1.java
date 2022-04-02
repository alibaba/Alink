package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinSupportCountDefaultAsNeg1<T> extends WithParams <T> {
	@NameCn("最小支持度数目")
	@DescCn("最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用")
	ParamInfo <Integer> MIN_SUPPORT_COUNT = ParamInfoFactory
		.createParamInfo("minSupportCount", Integer.class)
		.setDescription("Minimum support count")
		.setHasDefaultValue(-1)
		.build();

	default Integer getMinSupportCount() {
		return get(MIN_SUPPORT_COUNT);
	}

	default T setMinSupportCount(Integer value) {
		return set(MIN_SUPPORT_COUNT, value);
	}
}
