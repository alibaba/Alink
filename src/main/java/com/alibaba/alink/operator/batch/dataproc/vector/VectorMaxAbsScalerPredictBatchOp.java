package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;

/**
 * MaxAbsScaler transforms a dataSet of rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
public final class VectorMaxAbsScalerPredictBatchOp extends ModelMapBatchOp <VectorMaxAbsScalerPredictBatchOp>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerPredictBatchOp> {

	public VectorMaxAbsScalerPredictBatchOp() {
		this(new Params());
	}

	public VectorMaxAbsScalerPredictBatchOp(Params params) {
		super(VectorMaxAbsScalerModelMapper::new, params);
	}

}
