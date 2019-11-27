package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumSubsetFeatures;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

public interface RandomForestTrainParams<T> extends
	DecisionTreeTrainParams<T>,
	HasFeatureSubsamplingRatio<T>,
	HasNumSubsetFeatures<T>,
	HasNumTreesDefaltAs10<T>,
	HasSubsamplingRatio<T> {
}
