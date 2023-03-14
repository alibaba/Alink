package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;

public interface KModesTrainParams<T> extends
	HasFeatureCols <T>,
	HasKDefaultAs2 <T>,
	HasNumIterDefaultAs10 <T> {
}
