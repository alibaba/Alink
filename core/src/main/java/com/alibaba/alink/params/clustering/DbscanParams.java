package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasClusteringDistanceType;
import com.alibaba.alink.params.shared.clustering.HasEpsilon;
import com.alibaba.alink.params.shared.clustering.HasMinPoints;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.timeseries.HasIdCol;

public interface DbscanParams<T> extends
	HasClusteringDistanceType <T>,
	HasVectorCol <T>,
	HasIdCol <T>,
	HasPredictionCol <T>,
	HasMinPoints <T>,
	HasEpsilon <T> {
}
