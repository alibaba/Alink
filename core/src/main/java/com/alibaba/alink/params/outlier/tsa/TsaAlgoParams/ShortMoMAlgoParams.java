package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.outlier.tsa.HasAnomsNum;
import com.alibaba.alink.params.outlier.tsa.HasInfluence;
import com.alibaba.alink.params.outlier.tsa.HasTrainNum;

public interface ShortMoMAlgoParams<T> extends
	HasAnomsNum <T>,
	HasTrainNum <T>,
	HasInfluence <T> {
}
