package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.classification.IndividualTreeParams;
import com.alibaba.alink.params.shared.tree.HasNumSubsetFeatures;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSeed;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

public interface RandomForestRegTrainParams<T> extends
	IndividualTreeParams <T>,
	HasNumSubsetFeatures <T>,
	HasNumTreesDefaltAs10 <T>,
	HasSubsamplingRatio <T>,
	HasSeed <T> {
}
