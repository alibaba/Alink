package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface FtrlPredictParams<T> extends WithParams <T>,
	HasVectorColDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T> {
}
