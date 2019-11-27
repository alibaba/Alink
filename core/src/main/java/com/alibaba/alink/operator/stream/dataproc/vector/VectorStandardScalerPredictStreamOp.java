package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
public class VectorStandardScalerPredictStreamOp extends ModelMapStreamOp <VectorStandardScalerPredictStreamOp>
	implements VectorStandardTrainParams <VectorStandardScalerPredictStreamOp> {

	public VectorStandardScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public VectorStandardScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, VectorStandardScalerModelMapper::new, params);
	}

}
