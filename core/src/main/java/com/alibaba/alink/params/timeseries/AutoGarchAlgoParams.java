package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.timeseries.HasIcType;
import com.alibaba.alink.params.timeseries.HasIfGARCH11;
import com.alibaba.alink.params.timeseries.HasMaxOrder;
import com.alibaba.alink.params.timeseries.HasMinusMean;

public interface AutoGarchAlgoParams<T> extends
	HasIcType <T>,
	HasMaxOrder <T>,
	HasIfGARCH11 <T>,
	HasMinusMean <T> {
}