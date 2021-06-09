package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface GbdtPredictParams<T> extends
	DecisionTreePredictParams <T>,
	HasVectorColDefaultAsNull <T> {
}
