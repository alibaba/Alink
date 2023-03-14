package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ClusteringPredictParams<T> extends
	ModelMapperParams <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T> {
}
