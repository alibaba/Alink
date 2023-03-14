package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.outlier.tsa.HasInfluence;
import com.alibaba.alink.params.outlier.tsa.HasThreshold;
import com.alibaba.alink.params.outlier.tsa.HasTrainNum;

public interface SmoothZScoreAlgoParams<T> extends
	HasInfluence <T>,
	HasTrainNum <T>,
	HasThreshold <T> {
}
