package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;

/**
 * MaxAbsScaler transforms a dataset of Vector rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsPredict will scale the dataset with model which trained from MaxAbsTrain.
 */
public class MaxAbsScalerPredictStreamOp extends ModelMapStreamOp <MaxAbsScalerPredictStreamOp>
	implements MaxAbsScalerTrainParams <MaxAbsScalerPredictStreamOp> {

	public MaxAbsScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public MaxAbsScalerPredictStreamOp(BatchOperator model, Params params) {
		super(model, MaxAbsScalerModelMapper::new, params);
	}

}
