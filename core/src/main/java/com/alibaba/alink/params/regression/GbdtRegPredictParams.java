package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface GbdtRegPredictParams<T> extends
	RegPredictParams <T>,
	HasVectorColDefaultAsNull <T> {
}
