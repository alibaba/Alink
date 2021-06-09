package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.MultiHotModelMapper;
import com.alibaba.alink.params.feature.MultiHotPredictParams;

/**
 *  Multi hot encoding predict process.
 */
public class MultiHotPredictBatchOp extends ModelMapBatchOp <MultiHotPredictBatchOp>
	implements MultiHotPredictParams<MultiHotPredictBatchOp> {
	private static final long serialVersionUID = -6029385456358959482L;

	public MultiHotPredictBatchOp() {
		this(new Params());
	}

	public MultiHotPredictBatchOp(Params params) {
		super(MultiHotModelMapper::new, params);
	}
}
