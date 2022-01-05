package com.alibaba.alink.pipeline.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.TFTableModelTrainParams;
import com.alibaba.alink.params.tensorflow.savedmodel.HasInferSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * A general trainer to train a TensorFlow model from custom TensorFlow (version 1.15) scripts and to produce a {@link
 * TFTableModelPredictor}.
 * <p>
 * Note: As the parameter {@link HasSelectedColsDefaultAsNull} is used in both the trainer and predictor but with
 * different meanings. An additional parameter {@link HasInferSelectedColsDefaultAsNull} is added to specify inference
 * columns, and {@link TFTableModelTrainer#fit} is overridden for setting this parameter to the predictor.
 */
public class TFTableModelTrainer extends Trainer <TFTableModelTrainer, TFTableModelPredictor>
	implements TFTableModelTrainParams <TFTableModelTrainer>, TFTableModelPredictParams <TFTableModelTrainer>,
	HasInferSelectedColsDefaultAsNull <TFTableModelTrainer> {

	public TFTableModelTrainer() {this(null);}

	public TFTableModelTrainer(Params params) {
		super(params);
	}

	@Override
	public TFTableModelPredictor fit(BatchOperator <?> input) {
		TFTableModelPredictor predictor = super.fit(input);
		predictor.setSelectedCols(getInferSelectedCols());
		return predictor;
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new TFTableModelTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
