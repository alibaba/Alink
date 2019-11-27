package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
public final class VectorMinMaxScalerPredictBatchOp extends ModelMapBatchOp <VectorMinMaxScalerPredictBatchOp>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerPredictBatchOp> {

	public VectorMinMaxScalerPredictBatchOp() {
		this(new Params());
	}

	public VectorMinMaxScalerPredictBatchOp(Params params) {
		super(VectorMinMaxScalerModelMapper::new, params);
	}

}
