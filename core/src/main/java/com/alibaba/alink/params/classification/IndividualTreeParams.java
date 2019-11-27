package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.tree.HasCreateTreeMode;
import com.alibaba.alink.params.shared.tree.HasMaxBins;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxMemoryInMB;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;
import com.alibaba.alink.params.shared.tree.TreeTrainParams;

public interface IndividualTreeParams<T> extends
	ClassifierTrainParams<T>,
	TreeTrainParams<T>,
	HasMaxDepth<T>,
	HasMinSamplesPerLeaf<T>,
	HasCreateTreeMode<T>,
	HasMaxBins<T>,
	HasMaxMemoryInMB<T> {
}
