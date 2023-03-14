package com.alibaba.alink.params.outlier.tsa.baseparams;

import com.alibaba.alink.params.outlier.tsa.HasPredictCol;
import com.alibaba.alink.params.outlier.tsa.HasPredictNum;
import com.alibaba.alink.params.outlier.tsa.HasTimeSequenceCol;
import com.alibaba.alink.params.outlier.tsa.HasTimeWindow;
import com.alibaba.alink.params.outlier.tsa.HasTrainNum;
import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasValueCol;

public interface BaseStreamPredictParams<T> extends
	HasGroupColsDefaultAsNull <T>,
	HasTimeSequenceCol <T>,
	HasValueCol <T>,
	HasPredictCol <T>,
	HasTimeWindow <T>,
	HasTrainNum <T>,
	HasPredictNum <T>,
	HasReservedColsDefaultAsNull <T> {
}
