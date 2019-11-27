package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class QuantileDiscretizer extends Trainer <QuantileDiscretizer, QuantileDiscretizerModel>
	implements QuantileDiscretizerTrainParams <QuantileDiscretizer>,
	QuantileDiscretizerPredictParams <QuantileDiscretizer> {
	public QuantileDiscretizer() {
		super();
	}

	public QuantileDiscretizer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new QuantileDiscretizerTrainBatchOp(getParams()).linkFrom(in);
	}
}
