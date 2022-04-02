package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasIfGARCH11<T> extends WithParams <T> {

	@NameCn("是否用garch11")
	@DescCn("是否用garch11")
	ParamInfo <Boolean> IF_GARCH11 = ParamInfoFactory
		.createParamInfo("ifGARCH11", Boolean.class)
		.setDescription("ifGARCH11")
		.setHasDefaultValue(true)
		.build();

	default Boolean getIfGARCH11() {
		return get(IF_GARCH11);
	}

	default T setIfGARCH11(Boolean value) {
		return set(IF_GARCH11, value);
	}
}
