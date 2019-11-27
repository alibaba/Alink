package com.alibaba.alink.params.onlinelearning;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface FtrlPredictParams<T> extends WithParams<T>,
	HasVectorColDefaultAsNull <T>,
	HasReservedCols <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T> {
}
