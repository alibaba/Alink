package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.nlp.FeatureType;
import com.alibaba.alink.params.ParamUtil;

/**
 * FeatureType.
 */
public interface HasFeatureType<T> extends WithParams <T> {
	@NameCn("特征类型")
	@DescCn("生成特征向量的类型，支持IDF/WORD_COUNT/TF_IDF/Binary/TF")
	ParamInfo <FeatureType> FEATURE_TYPE = ParamInfoFactory
		.createParamInfo("featureType", FeatureType.class)
		.setDescription("Feature type, support IDF/WORD_COUNT/TF_IDF/Binary/TF")
		.setHasDefaultValue(FeatureType.WORD_COUNT)
		.build();

	default FeatureType getFeatureType() {
		return get(FEATURE_TYPE);
	}

	default T setFeatureType(FeatureType value) {
		return set(FEATURE_TYPE, value);
	}

	default T setFeatureType(String value) {
		return set(FEATURE_TYPE, ParamUtil.searchEnum(FEATURE_TYPE, value));
	}
}
