package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
public class VectorMinMaxScalerPredictStreamOp extends ModelMapStreamOp <VectorMinMaxScalerPredictStreamOp>
	implements VectorMinMaxScalerPredictParams <VectorMinMaxScalerPredictStreamOp> {

	private static final long serialVersionUID = -4616069594976834612L;

	public VectorMinMaxScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public VectorMinMaxScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, VectorMinMaxScalerModelMapper::new, params);
	}

}
