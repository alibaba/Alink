package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LinearSvmTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;
import com.alibaba.alink.params.classification.LinearSvmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Linear svm pipeline op.
 *
 */
public class LinearSvm extends Trainer <LinearSvm, LinearSvmModel>
	implements LinearSvmTrainParams <LinearSvm>, LinearSvmPredictParams <LinearSvm> {

	public LinearSvm() {
		super();
	}

	public LinearSvm(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LinearSvmTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
