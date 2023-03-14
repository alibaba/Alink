package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;

public interface SelectorPredictParams<T> extends
	HasPredictionCol <T>,
	HasSelectedColDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T> {
}
