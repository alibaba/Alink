package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatioDefaultAs1;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import com.alibaba.alink.params.shared.tree.HasMaxDepthDefaultAs6;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeafDefaultAs100;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaultAs100;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatioDefaultAs1;
import com.alibaba.alink.params.shared.tree.TreeTrainParams;

public interface GbdtTrainParams<T> extends
	ClassifierTrainParams <T>,
	TreeTrainParams <T>,
	HasNumTreesDefaultAs100<T>,
	HasMinSamplesPerLeafDefaultAs100<T>,
	HasMaxDepthDefaultAs6<T>,
	HasSubsamplingRatioDefaultAs1<T>,
	HasFeatureSubsamplingRatioDefaultAs1<T>,
	HasGroupColDefaultAsNull <T>,
	HasMaxBins <T> {

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
}

