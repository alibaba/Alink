package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSkip<T> extends WithParams <T> {

	@NameCn("每个分组超过最大样本数时，是否跳过")
	@DescCn("每个分组超过最大样本数时，是否跳过")
	ParamInfo <Boolean> SKIP = ParamInfoFactory
		.createParamInfo("skip", Boolean.class)
		.setDescription("skip the samples")
		.setHasDefaultValue(false)
		.build();

	default Boolean getSkip() {return get(SKIP);}

	default T setSkip(Boolean value) {return set(SKIP, value);}
}
