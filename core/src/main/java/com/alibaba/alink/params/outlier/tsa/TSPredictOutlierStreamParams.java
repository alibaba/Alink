package com.alibaba.alink.params.outlier.tsa;

import com.alibaba.alink.params.timeseries.AutoArimaAlgoParams;
import com.alibaba.alink.params.timeseries.HoltWintersAlgoParams;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.ShortMoMAlgoParams;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.SmoothZScoreAlgoParams;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseStreamPredictParams;
import com.alibaba.alink.params.timeseries.ArimaParamsOld;
import com.alibaba.alink.params.timeseries.AutoArimaGarchParams;

public interface TSPredictOutlierStreamParams<T> extends
	BaseStreamPredictParams <T>,
	HasInfluence <T>,
	AutoArimaAlgoParams<T>,
	ArimaParamsOld<T>,
	AutoArimaGarchParams <T>,
	HoltWintersAlgoParams <T>,
	SmoothZScoreAlgoParams <T>,
	ShortMoMAlgoParams <T>,
	HasOutlierCol<T>,
	TSPredictFuncTypeParams <T>,
	TSPredictOutlierTypeParams <T> {
}
