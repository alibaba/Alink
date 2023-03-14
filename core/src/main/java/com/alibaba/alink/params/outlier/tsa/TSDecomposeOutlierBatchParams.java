package com.alibaba.alink.params.outlier.tsa;

import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.BoxPlotAlgoParams;
import com.alibaba.alink.params.outlier.tsa.TsaAlgoParams.SHESDAlgoParams;

//stl   shesd, ksigma, boxplot.
public interface TSDecomposeOutlierBatchParams<T> extends
	STLParams <T>,
	ConvolutionsDecomposeParams <T>,
	BoxPlotAlgoParams <T>,
	SHESDAlgoParams <T>,
	HasOutlierCol <T>,
	TSDecomposeTimeSeriesFuncType <T>,
	TSDecomposeOutlierTypeParams <T> {}
