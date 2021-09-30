package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextPairClassifierTrainBatchOp;
import com.alibaba.alink.params.tensorflow.bert.BaseEasyTransferTrainParams;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.pipeline.Trainer;

public class BertTextPairClassifier extends Trainer <BertTextPairClassifier, BertClassificationModel>
	implements BaseEasyTransferTrainParams <BertTextPairClassifier>,
	TFTableModelClassificationPredictParams <BertTextPairClassifier> {

	public BertTextPairClassifier() {this(null);}

	public BertTextPairClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new BertTextPairClassifierTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
