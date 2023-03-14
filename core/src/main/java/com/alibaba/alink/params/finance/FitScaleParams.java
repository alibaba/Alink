package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredScoreCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

public interface FitScaleParams<T> extends
	HasLabelCol <T>,
	HasPositiveLabelValueString<T>,
	HasPredScoreCol <T>,
	HasScaledValue <T>,
	HasOdds <T>,
	HasPdo <T> {

	ParamInfo <Integer> TRUNCATE = ParamInfoFactory
		.createParamInfo("truncate", Integer.class)
		.setDescription("truncate")
		.setHasDefaultValue(0)
		.build();

	default Integer getTruncate() {
		return get(TRUNCATE);
	}

	default void setTruncate(Integer value) {
		set(TRUNCATE, value);
	}

	ParamInfo <String[]> EXCLUDE_FEATURE_COLS = ParamInfoFactory
		.createParamInfo("excludeFeatureCols", String[].class)
		.setDescription("excludeFeatureCols")
		.setHasDefaultValue(null)
		.build();

	default String[] getExcludeFeatureCols() {
		return get(EXCLUDE_FEATURE_COLS);
	}

	default void setExcludeFeatureCols(String[] value) {
		set(EXCLUDE_FEATURE_COLS, value);
	}

}
