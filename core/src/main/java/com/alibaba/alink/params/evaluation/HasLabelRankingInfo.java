package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.recommendation.KObjectUtil;

public interface HasLabelRankingInfo<T> extends WithParams <T> {
	ParamInfo <String> LABEL_RANKING_INFO = ParamInfoFactory
		.createParamInfo("labelRankingInfo", String.class)
		.setDescription("the label of ranking in label col")
		.setHasDefaultValue(KObjectUtil.OBJECT_NAME)
		.build();

	default String getLabelRankingInfo() {
		return get(LABEL_RANKING_INFO);
	}

	default T setLabelRankingInfo(String value) {
		return set(LABEL_RANKING_INFO, value);
	}
}
