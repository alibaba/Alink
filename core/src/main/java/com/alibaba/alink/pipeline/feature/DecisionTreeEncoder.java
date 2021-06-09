package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.C45TrainBatchOp;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.feature.DecisionTreeEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

public class DecisionTreeEncoder extends Trainer <DecisionTreeEncoder, DecisionTreeEncoderModel> implements
	DecisionTreeTrainParams <DecisionTreeEncoder>,
	DecisionTreeEncoderParams <DecisionTreeEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public DecisionTreeEncoder() {
	}

	public DecisionTreeEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new C45TrainBatchOp(this.getParams()).linkFrom(in);
	}
}
