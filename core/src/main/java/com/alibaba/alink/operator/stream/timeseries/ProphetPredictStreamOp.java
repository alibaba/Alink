package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.timeseries.ProphetModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.timeseries.ProphetPredictParams;

public class ProphetPredictStreamOp extends ModelMapStreamOp <ProphetPredictStreamOp>
	implements ProphetPredictParams <ProphetPredictStreamOp> {

	public ProphetPredictStreamOp() {
		this(null);
	}

	public ProphetPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public ProphetPredictStreamOp(BatchOperator model, Params params) {
		super(model, ProphetModelMapper::new, params);
	}

}
