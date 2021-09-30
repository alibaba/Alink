package com.alibaba.alink.pipeline.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.params.tensorflow.TFTableModelTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class TFTableModelTrainer extends Trainer <TFTableModelTrainer, TFTableModelPredictor>
	implements TFTableModelTrainParams <TFTableModelTrainer>, TFTableModelPredictParams <TFTableModelTrainer> {

	public TFTableModelTrainer() {this(null);}

	public TFTableModelTrainer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new TFTableModelTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
