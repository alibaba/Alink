package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.outlier.tsa.baseparams.BaseBatchPredictParams;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.AutoArimaGarchAlgoParams;

public interface AutoArimaGarchParams<T> extends
	BaseBatchPredictParams <T>,
	AutoArimaGarchAlgoParams <T> {
}
