package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierTrainBatchOp;
import com.alibaba.alink.params.tensorflow.kerasequential.BaseKerasSequentialTrainParams;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.pipeline.Trainer;

public class KerasSequentialClassifier extends Trainer <KerasSequentialClassifier, KerasSequentialClassificationModel>
	implements BaseKerasSequentialTrainParams <KerasSequentialClassifier>,
	TFTableModelClassificationPredictParams <KerasSequentialClassifier> {

	public KerasSequentialClassifier() {this(null);}

	public KerasSequentialClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new KerasSequentialClassifierTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
