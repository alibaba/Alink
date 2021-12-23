package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Params for IsotonicRegressionTrainer.
 */
public interface IsotonicRegTrainParams<T> extends
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T> {

	/**
	 * @cn-name 特征列名
	 * @cn 特征列的名称
	 */
	ParamInfo <String> FEATURE_COL = ParamInfoFactory
		.createParamInfo("featureCol", String.class)
		.setDescription("Name of the feature column。")
		.setAlias(new String[] {"featureColName"})
		.setHasDefaultValue(null)
		.build();
	/**
	 * @cn-name 输出序列是否
	 * @cn 输出序列是否递增
	 */
	ParamInfo <Boolean> ISOTONIC = ParamInfoFactory
		.createParamInfo("isotonic", Boolean.class)
		.setDescription("If true, the output sequence should be increasing!")
		.setHasDefaultValue(true)
		.build();
	/**
	 * @cn-name 训练特征所在维度
	 * @cn 训练特征在输入向量的维度索引
	 */
	ParamInfo <Integer> FEATURE_INDEX = ParamInfoFactory
		.createParamInfo("featureIndex", Integer.class)
		.setDescription("Feature index in the vector.")
		.setHasDefaultValue(0)
		.setValidator(new MinValidator <>(0))
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
