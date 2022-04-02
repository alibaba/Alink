package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSubSample<T> extends WithParams <T> {

	@NameCn("样本采样比例")
	@DescCn("样本采样比例")
	ParamInfo <Double> SUB_SAMPLE = ParamInfoFactory
		.createParamInfo("subSample", Double.class)
		.setDescription("Subsample ratio of the training instances.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getSubSample() {
		return get(SUB_SAMPLE);
	}

	default T setSubSample(Double eta) {
		return set(SUB_SAMPLE, eta);
	}
}
