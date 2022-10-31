package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface DbscanPredictParams<T> extends HasPredictionCol <T>, HasSelectedCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasNumThreads <T> {
}
