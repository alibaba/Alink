package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
@NameCn("归一化批预测")
public final class MinMaxScalerPredictLocalOp extends ModelMapLocalOp <MinMaxScalerPredictLocalOp>
	implements MinMaxScalerPredictParams <MinMaxScalerPredictLocalOp> {

	public MinMaxScalerPredictLocalOp() {
		this(new Params());
	}

	public MinMaxScalerPredictLocalOp(Params params) {
		super(MinMaxScalerModelMapper::new, params);
	}

}
