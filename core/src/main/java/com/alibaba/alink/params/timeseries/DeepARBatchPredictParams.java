package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.shared.HasTimeCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface DeepARBatchPredictParams<T> extends
	HasTimeCol <T>,
	HasSelectedCol <T>,
	HasVectorColDefaultAsNull <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasInferBatchSizeDefaultAs256 <T> {
}
