package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.outlier.HasBoxPlotK;
import com.alibaba.alink.params.outlier.tsa.HasBoxPlotRoundMode;

public interface BoxPlotAlgoParams<T> extends
	HasBoxPlotRoundMode <T>,
	HasBoxPlotK <T> {
}
