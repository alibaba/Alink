package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Quantile discretizer calculate the q-quantile as the interval, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public class QuantileDiscretizer extends Trainer<QuantileDiscretizer, QuantileDiscretizerModel>
	implements QuantileDiscretizerTrainParams<QuantileDiscretizer>,
	QuantileDiscretizerPredictParams<QuantileDiscretizer>,
	HasLazyPrintModelInfo<QuantileDiscretizer> {

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
