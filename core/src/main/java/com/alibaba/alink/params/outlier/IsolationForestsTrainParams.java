package com.alibaba.alink.params.outlier;

import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.tree.HasMaxDepth;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinSampleRatioPerChild;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeaf;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSeed;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

public interface IsolationForestsTrainParams<T> extends
	HasFeatureCols <T>,
	HasMaxLeaves <T>,
	HasMinSampleRatioPerChild <T>,
	HasMaxDepth <T>,
	HasMinSamplesPerLeaf <T>,
	HasNumTreesDefaltAs10 <T>,
	HasSubsamplingRatio <T>,
	HasSeed <T> {
}
