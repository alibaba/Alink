package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.tree.HasFeatureImportanceType;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatioDefaultAs1;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import com.alibaba.alink.params.shared.tree.HasMaxDepthDefaultAs6;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeafDefaultAs100;
import com.alibaba.alink.params.shared.tree.HasNewtonStep;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaultAs100;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatioDefaultAs1;
import com.alibaba.alink.params.shared.tree.TreeTrainParams;

public interface GbdtTrainParams<T> extends
	ClassifierTrainParams <T>,
	TreeTrainParams <T>,
	HasVectorColDefaultAsNull <T>,
	HasNumTreesDefaultAs100 <T>,
	HasMinSamplesPerLeafDefaultAs100 <T>,
	HasMaxDepthDefaultAs6 <T>,
	HasSubsamplingRatioDefaultAs1 <T>,
	HasFeatureSubsamplingRatioDefaultAs1 <T>,
	HasMaxBins <T>,
	HasNewtonStep <T>,
	HasFeatureImportanceType <T> {

	ParamInfo <Double> LEARNING_RATE = ParamInfoFactory
		.createParamInfo("learningRate", Double.class)
		.setDescription("learning rate for gbdt training(default 0.3)")
		.setHasDefaultValue(0.3)
		.build();
	ParamInfo <Double> MIN_SUM_HESSIAN_PER_LEAF = ParamInfoFactory
		.createParamInfo("minSumHessianPerLeaf", Double.class)
		.setDescription("minimum sum hessian for each leaf")
		.setHasDefaultValue(0.0)
		.build();

	ParamInfo <Double> LAMBDA = ParamInfoFactory
		.createParamInfo("lambda", Double.class)
		.setDescription("l1 reg in xgboost gain.")
		.setHasDefaultValue(0.0)
		.build();

	ParamInfo <Double> GAMMA = ParamInfoFactory
		.createParamInfo("gamma", Double.class)
		.setDescription("l2 reg in xgboost gain.")
		.setHasDefaultValue(0.0)
		.build();

	ParamInfo <CriteriaType> CRITERIA = ParamInfoFactory
		.createParamInfo("criteriaType", CriteriaType.class)
		.setHasDefaultValue(CriteriaType.PAI)
		.build();

	/**
	 * Indict the criteria type of tree model split.
	 */
	enum CriteriaType {
		/**
		 * Use gradient as the split criteria.
		 */
		PAI,

		/**
		 * Use gradient and hession as the split criteria.
		 */
		XGBOOST
	}

	default Double getLearningRate() {
		return get(LEARNING_RATE);
	}

	default T setLearningRate(Double value) {
		return set(LEARNING_RATE, value);
	}

	default Double getMinSumHessianPerLeaf() {
		return get(MIN_SUM_HESSIAN_PER_LEAF);
	}

	default T setMinSumHessianPerLeaf(Double value) {
		return set(MIN_SUM_HESSIAN_PER_LEAF, value);
	}

	default Double getLambda() {
		return get(LAMBDA);
	}

	default T setLambda(Double value) {
		return set(LAMBDA, value);
	}

	default Double getGamma() {
		return get(GAMMA);
	}

	default T setGamma(Double value) {
		return set(GAMMA, value);
	}

	default CriteriaType getCriteria() {
		return get(CRITERIA);
	}

	default T setCriteria(CriteriaType value) {
		return set(CRITERIA, value);
	}

	default T setCriteria(String value) {
		return set(CRITERIA, ParamUtil.searchEnum(CRITERIA, value));
	}
}

