package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasSamplingMethod<T> extends WithParams <T> {

	@NameCn("采样方法")
	@DescCn("采样方法")
	ParamInfo <SamplingMethod> SAMPLING_METHOD = ParamInfoFactory
		.createParamInfo("samplingMethod", SamplingMethod.class)
		.setDescription("The method to use to sample the training instances.")
		.setHasDefaultValue(SamplingMethod.UNIFORM)
		.build();

	default SamplingMethod getSamplingMethod() {
		return get(SAMPLING_METHOD);
	}

	default T setSamplingMethod(SamplingMethod samplingMethod) {
		return set(SAMPLING_METHOD, samplingMethod);
	}

	default T setSamplingMethod(String samplingMethod) {
		return set(SAMPLING_METHOD, ParamUtil.searchEnum(SAMPLING_METHOD, samplingMethod));
	}

	enum SamplingMethod {
		UNIFORM,
		GRADIENT_BASED
	}
}
