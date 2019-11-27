package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;
import com.alibaba.alink.params.classification.SoftmaxTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Softmax is a multi class classifier.
 *
 */
public class Softmax extends Trainer <Softmax, SoftmaxModel> implements
	SoftmaxTrainParams <Softmax>,
	SoftmaxPredictParams <Softmax> {

	public Softmax() {super();}

	public Softmax(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new SoftmaxTrainBatchOp(getParams()).linkFrom(in);
	}
}
