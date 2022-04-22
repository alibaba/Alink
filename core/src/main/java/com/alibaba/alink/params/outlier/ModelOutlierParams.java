package com.alibaba.alink.params.outlier;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ModelOutlierParams<T> extends
	ModelMapperParams <T>,
	OutlierDetectorParams <T>,
	HasReservedColsDefaultAsNull <T> {
}
