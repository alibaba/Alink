package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.KerasSequentialRegressorTrainBatchOp;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;
import com.alibaba.alink.params.tensorflow.kerasequential.BaseKerasSequentialTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Regressor using Keras Sequential model.
 */
@NameCn("KerasSequential回归")
public class KerasSequentialRegressor extends Trainer <KerasSequentialRegressor, KerasSequentialRegressionModel>
	implements BaseKerasSequentialTrainParams <KerasSequentialRegressor>,
	TFTableModelRegressionPredictParams <KerasSequentialRegressor> {

	public KerasSequentialRegressor() {this(null);}

	public KerasSequentialRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new KerasSequentialRegressorTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
