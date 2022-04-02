package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

interface ModelOutlierParams<T> extends
	ModelMapperParams <T>,
	OutlierDetectorParams <T>,
	HasReservedColsDefaultAsNull <T> {
}
