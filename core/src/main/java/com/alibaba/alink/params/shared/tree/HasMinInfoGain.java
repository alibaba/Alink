package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinInfoGain<T> extends WithParams <T> {
	@NameCn("分裂的最小增益")
	@DescCn("分裂的最小增益")
	ParamInfo <Double> MIN_INFO_GAIN = ParamInfoFactory
		.createParamInfo("minInfoGain", Double.class)
		.setDescription("minimum info gain when performing split")
		.setHasDefaultValue(0.0)
		.build();

	default Double getMinInfoGain() {
		return get(MIN_INFO_GAIN);
	}

	default T setMinInfoGain(Double value) {
		return set(MIN_INFO_GAIN, value);
	}
}
