package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.timeseries.LSTNetModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.timeseries.LSTNetPredictParams;

public class LSTNetPredictStreamOp extends ModelMapStreamOp <LSTNetPredictStreamOp>
	implements LSTNetPredictParams <LSTNetPredictStreamOp> {

	public LSTNetPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public LSTNetPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, LSTNetModelMapper::new, params);
	}
}
