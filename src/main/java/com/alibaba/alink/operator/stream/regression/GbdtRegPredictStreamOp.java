package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.GbdtRegPredictParams;

public class GbdtRegPredictStreamOp extends ModelMapStreamOp <GbdtRegPredictStreamOp>
	implements GbdtRegPredictParams <GbdtRegPredictStreamOp> {
	public GbdtRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public GbdtRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, GbdtModelMapper::new, params);
	}

}
