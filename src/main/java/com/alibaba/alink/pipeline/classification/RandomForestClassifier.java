package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.RandomForestPredictParams;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class RandomForestClassifier extends Trainer <RandomForestClassifier, RandomForestClassificationModel>
	implements RandomForestTrainParams <RandomForestClassifier>,
	RandomForestPredictParams <RandomForestClassifier> {

	public RandomForestClassifier() {
		super();
	}

	public RandomForestClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new RandomForestTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
