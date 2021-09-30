package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.BertTextPairRegressorTrainBatchOp;
import com.alibaba.alink.params.tensorflow.bert.BaseEasyTransferTrainParams;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.pipeline.Trainer;

public class BertTextPairRegressor extends Trainer <BertTextPairRegressor, BertRegressionModel>
	implements BaseEasyTransferTrainParams <BertTextPairRegressor>,
	TFTableModelClassificationPredictParams <BertTextPairRegressor> {

	public BertTextPairRegressor() {this(null);}

	public BertTextPairRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new BertTextPairRegressorTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
