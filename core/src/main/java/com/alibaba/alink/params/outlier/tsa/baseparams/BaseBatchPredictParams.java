package com.alibaba.alink.params.outlier.tsa.baseparams;

import com.alibaba.alink.params.outlier.tsa.HasPredictCol;
import com.alibaba.alink.params.outlier.tsa.HasPredictNum;
import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasValueCol;

public interface BaseBatchPredictParams<T> extends
	HasGroupColDefaultAsNull <T>,
	HasValueCol <T>,
	HasPredictCol <T>,
	HasPredictNum <T> {}
