package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
public class MinMaxScalerPredictStreamOp extends ModelMapStreamOp <MinMaxScalerPredictStreamOp>
	implements MinMaxScalerPredictParams <MinMaxScalerPredictStreamOp> {

	private static final long serialVersionUID = -4043583536803216948L;

	public MinMaxScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public MinMaxScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, MinMaxScalerModelMapper::new, params);
	}

}
