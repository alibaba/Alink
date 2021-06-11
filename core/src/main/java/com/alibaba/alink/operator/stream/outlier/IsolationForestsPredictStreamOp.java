package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.IForestsModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.outlier.IsolationForestsPredictParams;

public class IsolationForestsPredictStreamOp extends ModelMapStreamOp <IsolationForestsPredictStreamOp>
	implements IsolationForestsPredictParams <IsolationForestsPredictStreamOp> {
	private static final long serialVersionUID = -1094746719364729720L;

	public IsolationForestsPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public IsolationForestsPredictStreamOp(BatchOperator model, Params params) {
		super(model, IForestsModelMapper::new, params);
	}
}
