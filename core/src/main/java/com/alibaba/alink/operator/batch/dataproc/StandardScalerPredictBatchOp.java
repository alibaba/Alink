package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelMapper;
import com.alibaba.alink.params.dataproc.StandardPredictParams;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
public final class StandardScalerPredictBatchOp extends ModelMapBatchOp <StandardScalerPredictBatchOp>
	implements StandardPredictParams <StandardScalerPredictBatchOp> {

	private static final long serialVersionUID = -3667702053244411457L;

	public StandardScalerPredictBatchOp() {
		this(new Params());
	}

	public StandardScalerPredictBatchOp(Params params) {
		super(StandardScalerModelMapper::new, params);
	}

}
