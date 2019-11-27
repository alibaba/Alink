package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.GbdtPredictParams;

public class GbdtPredictStreamOp extends ModelMapStreamOp <GbdtPredictStreamOp>
	implements GbdtPredictParams <GbdtPredictStreamOp> {
	public GbdtPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public GbdtPredictStreamOp(BatchOperator model, Params params) {
		super(model, GbdtModelMapper::new, params);
	}
}

