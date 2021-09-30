package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface LSTNetBatchPredictParams<T> extends
	HasSelectedCol <T>, HasPredictionCol <T>, HasReservedColsDefaultAsNull <T>,
	HasInferBatchSizeDefaultAs256 <T> {
}
