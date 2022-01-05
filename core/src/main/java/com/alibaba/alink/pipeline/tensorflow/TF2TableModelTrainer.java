package com.alibaba.alink.pipeline.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.TF2TableModelTrainParams;
import com.alibaba.alink.params.tensorflow.savedmodel.HasInferSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * A general trainer to train a TensorFlow model from custom TensorFlow (version 2.3.1) scripts and to produce a {@link
 * TFTableModelPredictor}.
 * <p>
 * Note: As the parameter {@link HasSelectedColsDefaultAsNull} is used in both the trainer and predictor but with
 * different meanings. An additional parameter {@link HasInferSelectedColsDefaultAsNull} is added to specify inference
 * columns, and {@link TF2TableModelTrainer#fit} is overridden for setting this parameter to the predictor.
 */
public class TF2TableModelTrainer extends Trainer <TF2TableModelTrainer, TFTableModelPredictor>
	implements TF2TableModelTrainParams <TF2TableModelTrainer>, TFTableModelPredictParams <TF2TableModelTrainer>,
	HasInferSelectedColsDefaultAsNull <TF2TableModelTrainer> {

	public TF2TableModelTrainer() {this(null);}

	public TF2TableModelTrainer(Params params) {
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
		return new TF2TableModelTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
