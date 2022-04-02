package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;

interface OutlierParams<T> extends OutlierDetectorParams <T>,
	HasGroupColsDefaultAsNull <T> {
}