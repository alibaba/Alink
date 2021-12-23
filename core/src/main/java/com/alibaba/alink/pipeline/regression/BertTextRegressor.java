package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.BertTextRegressorTrainBatchOp;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.bert.BertTextTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Text regressor using Bert models.
 */
public class BertTextRegressor extends Trainer <BertTextRegressor, BertRegressionModel>
	implements BertTextTrainParams <BertTextRegressor>,
	TFTableModelClassificationPredictParams <BertTextRegressor> {

	public BertTextRegressor() {this(null);}

	public BertTextRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new BertTextRegressorTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
