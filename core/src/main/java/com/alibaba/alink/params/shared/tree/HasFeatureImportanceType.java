package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasFeatureImportanceType<T> extends WithParams <T> {

	ParamInfo <FeatureImportanceType> FEATURE_IMPORTANCE_TYPE = ParamInfoFactory
		.createParamInfo("featureImportanceType", FeatureImportanceType.class)
		.setHasDefaultValue(FeatureImportanceType.GAIN)
		.build();

	/**
	 * Indict the feature importance type of tree model.
	 */
	enum FeatureImportanceType {
		/**
		 * The number of times a feature is used to split the data across all trees.
		 */
		WEIGHT,

		/**
		 * The average gain across all splits the feature is used in.
		 */
		GAIN,

		/**
		 * the average coverage across all splits the feature is used in.
		 */
		COVER
	}

	default HasFeatureImportanceType.FeatureImportanceType getFeatureImportanceType() {
		return get(HasFeatureImportanceType.FEATURE_IMPORTANCE_TYPE);
	}

	default T setFeatureImportanceType(HasFeatureImportanceType.FeatureImportanceType featureImportanceType) {
		return set(HasFeatureImportanceType.FEATURE_IMPORTANCE_TYPE, featureImportanceType);
	}

	default T setFeatureImportanceType(String value) {
		return set(HasFeatureImportanceType.FEATURE_IMPORTANCE_TYPE,
			ParamUtil.searchEnum(HasFeatureImportanceType.FEATURE_IMPORTANCE_TYPE, value));
	}
}
