package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

/**
 * Params for IsotonicRegressionTrainer.
 */
public interface IsotonicRegTrainParams<T> extends
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T> {

	ParamInfo <String> FEATURE_COL = ParamInfoFactory
		.createParamInfo("featureCol", String.class)
		.setDescription("Name of the feature column。")
		.setAlias(new String[]{"featureColName"})
		.setHasDefaultValue(null)
		.build();
	ParamInfo <Boolean> ISOTONIC = ParamInfoFactory
		.createParamInfo("isotonic", Boolean.class)
		.setDescription("If true, the output sequence should be increasing!")
		.setHasDefaultValue(true)
		.build();
	ParamInfo <Integer> FEATURE_INDEX = ParamInfoFactory
		.createParamInfo("featureIndex", Integer.class)
		.setDescription("Feature index in the vector.")
		.setHasDefaultValue(0)
		.build();

	default String getFeatureCol() {
		return get(FEATURE_COL);
	}

	default T setFeatureCol(String value) {
		return set(FEATURE_COL, value);
	}

	default Boolean getIsotonic() {
		return get(ISOTONIC);
	}

	default T setIsotonic(Boolean value) {
		return set(ISOTONIC, value);
	}

	default Integer getFeatureIndex() {
		return get(FEATURE_INDEX);
	}

	default T setFeatureIndex(Integer value) {
		return set(FEATURE_INDEX, value);
	}

}
