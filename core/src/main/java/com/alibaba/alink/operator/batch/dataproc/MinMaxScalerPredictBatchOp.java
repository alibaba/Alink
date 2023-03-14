package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@NameCn("归一化批预测")
@NameEn("Min Max Scaler Batch Predict")
public final class MinMaxScalerPredictBatchOp extends ModelMapBatchOp <MinMaxScalerPredictBatchOp>
	implements MinMaxScalerPredictParams <MinMaxScalerPredictBatchOp> {

	private static final long serialVersionUID = -4480727458655762970L;

	public MinMaxScalerPredictBatchOp() {
		this(new Params());
	}

	public MinMaxScalerPredictBatchOp(Params params) {
		super(MinMaxScalerModelMapper::new, params);
	}

}
