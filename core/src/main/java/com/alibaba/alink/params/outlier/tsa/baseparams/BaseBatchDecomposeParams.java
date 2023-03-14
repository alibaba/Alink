package com.alibaba.alink.params.outlier.tsa.baseparams;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.params.outlier.tsa.HasOutlierCol;
import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;
import com.alibaba.alink.params.timeseries.HasValueCol;

public interface BaseBatchDecomposeParams<T> extends
	HasGroupColsDefaultAsNull <T>,
	HasValueCol <T>,
	HasOutlierCol <T>,
	BaseDecomposeOutputParams <T> {}
