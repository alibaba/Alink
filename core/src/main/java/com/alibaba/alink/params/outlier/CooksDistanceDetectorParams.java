package com.alibaba.alink.params.outlier;

import com.alibaba.alink.params.shared.colname.HasLabelColDefaultAsNull;

public interface CooksDistanceDetectorParams<T> extends
	OutlierDetectorParams <T>,
	WithMultiVarParams <T>,
	HasLabelColDefaultAsNull <T> {
}
