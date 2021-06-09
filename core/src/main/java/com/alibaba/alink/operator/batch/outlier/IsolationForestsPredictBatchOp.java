package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.IForestsModelMapper;
import com.alibaba.alink.params.outlier.IsolationForestsPredictParams;

public final class IsolationForestsPredictBatchOp extends ModelMapBatchOp <IsolationForestsPredictBatchOp>
	implements IsolationForestsPredictParams <IsolationForestsPredictBatchOp> {
	private static final long serialVersionUID = 1003911063515453520L;

	public IsolationForestsPredictBatchOp() {
		this(null);
	}

	public IsolationForestsPredictBatchOp(Params params) {
		super(IForestsModelMapper::new, params);
	}
}
