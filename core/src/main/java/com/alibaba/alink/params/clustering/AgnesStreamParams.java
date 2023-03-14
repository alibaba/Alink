package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasDistanceThreshold;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface AgnesStreamParams<T> extends
	HasPredictionCol <T>,
	HasDistanceThreshold <T>,
	HasReservedColsDefaultAsNull <T> {
}
