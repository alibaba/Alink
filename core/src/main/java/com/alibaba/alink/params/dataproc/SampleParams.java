package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.RangeValidator;

public interface SampleParams<T> extends
	WithParams <T> {

	@NameCn("采样比例")
	@DescCn("采样率，范围为[0, 1]")
	ParamInfo <Double> RATIO = ParamInfoFactory
		.createParamInfo("ratio", Double.class)
		.setDescription("sampling ratio, it should be in range of [0, 1]")
		.setRequired()
		.setAlias(new String[] {"sampleRate"})
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	@NameCn("是否放回")
	@DescCn("是否有放回的采样，默认不放回")
	ParamInfo <Boolean> WITH_REPLACEMENT = ParamInfoFactory
		.createParamInfo("withReplacement", Boolean.class)
		.setDescription("Indicates whether to enable sampling with replacement, default is without replcement")
		.setHasDefaultValue(false)
		.build();

	default Double getRatio() {
		return getParams().get(RATIO);
	}

	default T setRatio(Double value) {
		return set(RATIO, value);
	}

	default Boolean getWithReplacement() {
		return getParams().get(WITH_REPLACEMENT);
	}

	default T setWithReplacement(Boolean value) {
		return set(WITH_REPLACEMENT, value);
	}

}
