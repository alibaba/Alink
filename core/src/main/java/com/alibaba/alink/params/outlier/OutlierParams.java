package com.alibaba.alink.params.outlier;

import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;

public interface OutlierParams<T> extends OutlierDetectorParams <T>,
	HasGroupColsDefaultAsNull <T> {
}