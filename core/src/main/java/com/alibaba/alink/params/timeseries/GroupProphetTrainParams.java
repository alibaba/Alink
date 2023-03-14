package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.shared.colname.HasGroupCols;

public interface GroupProphetTrainParams<T> extends
	ProphetTrainParams <T>,
	HasGroupCols<T> {
}
