package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.vector.VectorMinMaxScalerTrainLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer normalizes the value of the vector to [0,1] using the following formula:
 *
 * x_scaled = (x - eMin) / (eMax - eMin) * (maxV - minV) + minV;
 */
@NameCn("向量归一化")
public class VectorMinMaxScaler extends Trainer <VectorMinMaxScaler, VectorMinMaxScalerModel> implements
	VectorMinMaxScalerTrainParams <VectorMinMaxScaler>,
	VectorMinMaxScalerPredictParams <VectorMinMaxScaler>,
	HasLazyPrintModelInfo <VectorMinMaxScaler> {

	private static final long serialVersionUID = -4425448502218692981L;

	public VectorMinMaxScaler() {
		this(new Params());
	}

	public VectorMinMaxScaler(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new VectorMinMaxScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new VectorMinMaxScalerTrainLocalOp(this.getParams()).linkFrom(in);
	}
}

