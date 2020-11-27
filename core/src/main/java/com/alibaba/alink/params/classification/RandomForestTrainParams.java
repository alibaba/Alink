package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumSubsetFeatures;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasNumTreesOfGini;
import com.alibaba.alink.params.shared.tree.HasNumTreesOfInfoGain;
import com.alibaba.alink.params.shared.tree.HasNumTreesOfInfoGainRatio;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

public interface RandomForestTrainParams<T> extends
	IndividualTreeParams <T>,
	HasFeatureSubsamplingRatio <T>,
	HasNumSubsetFeatures <T>,
	HasNumTreesDefaltAs10 <T>,
	HasNumTreesOfGini <T>,
	HasNumTreesOfInfoGain <T>,
	HasNumTreesOfInfoGainRatio <T>,
	HasSubsamplingRatio <T> {
}
