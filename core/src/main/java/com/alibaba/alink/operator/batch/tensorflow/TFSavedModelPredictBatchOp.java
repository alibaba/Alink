package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.tensorflow.TFSavedModelPredictMapper;
import com.alibaba.alink.params.tensorflow.savedmodel.TFSavedModelPredictParams;

/**
 * This operator loads a tensorflow SavedModel, and performs prediction with Tensorflow's Java sdk.
 */
public final class TFSavedModelPredictBatchOp extends MapBatchOp <TFSavedModelPredictBatchOp>
	implements TFSavedModelPredictParams <TFSavedModelPredictBatchOp> {

	public TFSavedModelPredictBatchOp() {
		this(new Params());
	}

	public TFSavedModelPredictBatchOp(Params params) {
		super(TFSavedModelPredictMapper::new, params);
	}
}
