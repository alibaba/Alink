package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSparseVectorCol;

public interface ExclusiveFeatureBundlePredictParams<T> extends
	HasSparseVectorCol <T>,
	HasReservedColsDefaultAsNull <T> {
}
