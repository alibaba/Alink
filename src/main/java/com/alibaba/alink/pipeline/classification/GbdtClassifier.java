package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.GbdtTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.GbdtPredictParams;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class GbdtClassifier extends Trainer <GbdtClassifier, GbdtClassificationModel> implements
	GbdtTrainParams <GbdtClassifier>,
	GbdtPredictParams <GbdtClassifier> {

	public GbdtClassifier() {
		this(new Params());
	}

	public GbdtClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new GbdtTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
