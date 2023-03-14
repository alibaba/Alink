package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.outlier.HasDirection;
import com.alibaba.alink.params.outlier.tsa.HasMaxAnoms;
import com.alibaba.alink.params.outlier.tsa.HasSHESDAlpha;
import com.alibaba.alink.params.timeseries.HasFrequency;

public interface SHESDAlgoParams<T> extends
	HasFrequency <T>,
	HasSHESDAlpha <T>,
	HasMaxAnoms <T>,
	HasDirection <T> {
}
