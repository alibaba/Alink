package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.timeseries.ProphetModelMapper;
import com.alibaba.alink.params.timeseries.ProphetPredictParams;

public class ProphetPredictBatchOp extends ModelMapBatchOp<ProphetPredictBatchOp>
	implements ProphetPredictParams<ProphetPredictBatchOp> {

	public ProphetPredictBatchOp() {
		this(null);
	}

	public ProphetPredictBatchOp(Params params) {
		super(ProphetModelMapper::new, params);
	}

}
