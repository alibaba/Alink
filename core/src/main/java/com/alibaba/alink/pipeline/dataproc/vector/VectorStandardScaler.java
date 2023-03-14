package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer standard the value of the vector using the following formula:
 *
 * x_scaled = (x - mean)／sigma, where mean is the mean value of column, sigma is the standard variance.
 */
@NameCn("向量标准化")
public class VectorStandardScaler extends Trainer <VectorStandardScaler, VectorStandardScalerModel> implements
	VectorStandardTrainParams <VectorStandardScaler>,
	VectorStandardPredictParams <VectorStandardScaler>,
	HasLazyPrintModelInfo <VectorStandardScaler> {

	private static final long serialVersionUID = -4030303533710714585L;

	public VectorStandardScaler() {
		super();
	}

	public VectorStandardScaler(Params params) {
		super(params);
	}

}

