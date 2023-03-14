package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.timeseries.HasArimaGarchMethod;
import com.alibaba.alink.params.timeseries.HasIcType;
import com.alibaba.alink.params.timeseries.HasIfGARCH11;
import com.alibaba.alink.params.timeseries.HasMaxArima;
import com.alibaba.alink.params.timeseries.HasMaxGarch;

public interface AutoArimaGarchAlgoParams<T> extends
	HasIcType <T>,
	HasMaxArima <T>,
	HasMaxGarch <T>,
	HasIfGARCH11 <T>,
	HasArimaGarchMethod <T> {
}