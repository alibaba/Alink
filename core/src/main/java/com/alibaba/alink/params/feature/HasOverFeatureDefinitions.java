package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.fe.define.BaseStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceLatestStatFeatures;

public interface HasOverFeatureDefinitions<T> extends WithParams <T> {

	@NameCn("特征定义")
	@DescCn("特征的具体描述")
	ParamInfo <String> FEATURE_DEFINITIONS = ParamInfoFactory
		.createParamInfo("featureDefinitions", String.class)
		.setDescription("Definitions for new features.")
		.setRequired()
		.build();

	default String getFeatureDefinitions() {
		return get(FEATURE_DEFINITIONS);
	}

	default T setFeatureDefinitions(InterfaceLatestStatFeatures... definitions) {
		return set(FEATURE_DEFINITIONS, BaseStatFeatures.toJson(definitions));
	}
}
