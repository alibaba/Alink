package com.alibaba.alink.params.outlier;

public interface Outlier4GroupedDataParams<T> extends OutlierDetectorParams <T>,
	HasInputMTableCol <T>,
	HasOutputMTableCol <T>,
	HasMaxOutlierRatio <T>,
	HasMaxOutlierNumPerGroup <T> {
}
