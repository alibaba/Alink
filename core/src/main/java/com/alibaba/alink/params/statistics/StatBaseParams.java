package com.alibaba.alink.params.statistics;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.shared.HasTimeIntervalDefaultAs3;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
public interface StatBaseParams<T> extends
	HasSelectedColsDefaultAsNull <T>,
	HasTimeIntervalDefaultAs3 <T> {
}
