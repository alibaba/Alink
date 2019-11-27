package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.DecisionTreeTrainBatchOp;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class DecisionTreeClassifier extends Trainer<DecisionTreeClassifier, DecisionTreeClassificationModel> implements
	DecisionTreeTrainParams<DecisionTreeClassifier>,
	DecisionTreePredictParams<DecisionTreeClassifier> {

	public DecisionTreeClassifier() {
		super();
	}

	public DecisionTreeClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new DecisionTreeTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
