package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinPercent<T> extends WithParams <T> {
	@NameCn("最小百分比阈值")
	@DescCn("最小百分比阈值，当minFrequency取值小于0时起作用，默认值-1")
	ParamInfo <Double> MIN_PERCENT = ParamInfoFactory
		.createParamInfo("minPercent", Double.class)
		.setDescription("Minimum percent threshold")
		.setHasDefaultValue(-1.0)
		.build();

	default Double getMinPercent() {
		return get(MIN_PERCENT);
	}

	default T setMinPercent(Double value) {
		return set(MIN_PERCENT, value);
	}
}
