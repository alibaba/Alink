package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.StandardPredictParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
public class StandardScalerPredictStreamOp extends ModelMapStreamOp <StandardScalerPredictStreamOp>
	implements StandardPredictParams <StandardScalerPredictStreamOp> {

	public StandardScalerPredictStreamOp(BatchOperator srt) {
		this(srt, new Params());
	}

	public StandardScalerPredictStreamOp(BatchOperator srt, Params params) {
		super(srt, StandardScalerModelMapper::new, params);
	}

}
