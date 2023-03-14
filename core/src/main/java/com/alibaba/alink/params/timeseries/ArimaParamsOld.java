package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.outlier.tsa.baseparams.BaseBatchPredictParams;

public interface ArimaParamsOld<T> extends
	BaseBatchPredictParams <T>,
	ArimaAlgoParams <T> {
}
