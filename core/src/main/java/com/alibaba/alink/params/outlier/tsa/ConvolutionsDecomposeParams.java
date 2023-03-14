package com.alibaba.alink.params.outlier.tsa;

import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.ConvolutionDecomposeAlgoParams;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseBatchDecomposeParams;

public interface ConvolutionsDecomposeParams<T> extends
	ConvolutionDecomposeAlgoParams <T>,
	BaseBatchDecomposeParams <T> {
}
