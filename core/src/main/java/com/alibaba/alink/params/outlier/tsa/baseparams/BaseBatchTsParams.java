package com.alibaba.alink.params.outlier.tsa.baseparams;

import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasValueCol;

public interface BaseBatchTsParams<T> extends
	HasGroupColsDefaultAsNull <T>,
	HasValueCol <T> {}
