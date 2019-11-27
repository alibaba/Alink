package com.alibaba.alink.params.shared.tree;

import com.alibaba.alink.params.shared.colname.HasCategoricalCols;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

public interface TreeTrainParams<T> extends
	HasCategoricalCols <T>,
	HasWeightColDefaultAsNull <T>,
	HasMaxLeaves <T>,
	HasMinSampleRatioPerChild <T>,
	HasMinInfoGain <T> {
}
