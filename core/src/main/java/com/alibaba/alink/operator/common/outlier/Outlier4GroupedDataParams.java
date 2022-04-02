package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.params.outlier.HasMaxOutlierNumPerGroup;
import com.alibaba.alink.params.outlier.HasMaxOutlierRatio;

interface Outlier4GroupedDataParams<T> extends OutlierDetectorParams <T>,
	HasInputMTableCol <T>,
	HasOutputMTableCol <T>,
	HasMaxOutlierRatio <T>,
	HasMaxOutlierNumPerGroup <T> {
}
