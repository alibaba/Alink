package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.fe.def.day.BaseDaysStatFeatures;
import com.alibaba.alink.params.shared.colname.HasTimeCol;

public interface GenerateFeatureOfLatestDayParams<T> extends HasTimeCol <T> {
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

	default T setFeatureDefinitions(BaseDaysStatFeatures <?>... definitions) {
		return set(FEATURE_DEFINITIONS, BaseDaysStatFeatures.toJson(definitions));
	}

	default T setFeatureDefinitions(String definitions) {
		return set(FEATURE_DEFINITIONS, definitions);
	}

	@NameCn("截止日期,格式是yyyy-mm-dd")
	@DescCn("截止日期,格式是yyyy-mm-dd")
	ParamInfo <String> BASE_DATE = ParamInfoFactory
		.createParamInfo("baseDate", String.class)
		.setDescription("base date for end time")
		.setRequired()
		.build();

	default String getBaseDate() {
		return get(BASE_DATE);
	}

	default T setBaseDate(String baseData) {
		return set(BASE_DATE, baseData);
	}

	@NameCn("扩展特征")
	@DescCn("扩展特征")
	ParamInfo <String> EXTEND_FEATURES = ParamInfoFactory
		.createParamInfo("extendFeatures", String.class)
		.setDescription("extend features.")
		.setHasDefaultValue(null)
		.build();

	default String getExtendFeatures() {
		return get(EXTEND_FEATURES);
	}

	default T setExtendFeatures(String extendFeatures) {
		return set(EXTEND_FEATURES, extendFeatures);
	}

}
