package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer normalizes the value of the vector to [-1,1] using the following formula:
 *
 * x_scaled = x / abs(X_max)
 */
@NameCn("向量绝对值最大化")
public class VectorMaxAbsScaler extends Trainer <VectorMaxAbsScaler, VectorMaxAbsScalerModel> implements
	VectorMaxAbsScalerTrainParams <VectorMaxAbsScaler>,
	VectorMaxAbsScalerPredictParams <VectorMaxAbsScaler>,
	HasLazyPrintModelInfo <VectorMaxAbsScaler> {

	private static final long serialVersionUID = 6824636058441367182L;

	public VectorMaxAbsScaler() {
		super();
	}

	public VectorMaxAbsScaler(Params params) {
		super(params);
	}

}

