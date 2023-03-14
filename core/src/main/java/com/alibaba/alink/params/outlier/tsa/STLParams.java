package com.alibaba.alink.params.outlier.tsa;

import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.STLAlgoParams;
import com.alibaba.alink.params.outlier.tsa.baseparams.BaseBatchDecomposeParams;

public interface STLParams<T> extends
	STLAlgoParams <T>,
	BaseBatchDecomposeParams <T> {
}
