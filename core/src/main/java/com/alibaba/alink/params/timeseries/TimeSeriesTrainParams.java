package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.shared.colname.HasTimeCol;

@ParamSelectColumnSpec(name = "valueCol", allowedTypeCollections= TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "timeCol", allowedTypeCollections= TypeCollections.TIMESTAMP_TYPES)
public interface TimeSeriesTrainParams<T> extends
	HasValueCol <T>,
	HasTimeCol <T> {
}
