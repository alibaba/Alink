package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.HasTimeInterval;
import com.alibaba.alink.params.shared.clustering.HasHalfLife;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface StreamingKMeansParams<T> extends
	HasPredictionCol <T>,
	HasHalfLife <T>,
	HasPredictionDistanceCol<T>,
	HasPredictionClusterCol<T>,
	HasReservedColsDefaultAsNull <T>,
	HasTimeInterval <T> {
}
