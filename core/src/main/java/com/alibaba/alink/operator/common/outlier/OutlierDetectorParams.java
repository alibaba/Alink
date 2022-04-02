package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.outlier.HasOutlierThreshold;
import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;

public interface OutlierDetectorParams<T> extends MapperParams <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasOutlierThreshold <T> {
}