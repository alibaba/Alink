package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface GbdtRegPredictParams<T> extends
	HasVectorColDefaultAsNull <T>, RegPredictParams <T>, HasNumThreads <T> {
}
