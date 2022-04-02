package com.alibaba.alink.operator.stream.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;

/**
 * This operator loads a tensorflow SavedModel model wrapped in Alink Model format, and performs prediction with
 * Tensorflow's Java sdk.
 */
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("TF表模型预测")
public final class TFTableModelPredictStreamOp extends ModelMapStreamOp <TFTableModelPredictStreamOp>
	implements TFTableModelPredictParams <TFTableModelPredictStreamOp> {

	public TFTableModelPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public TFTableModelPredictStreamOp(BatchOperator model, Params params) {
		super(model, TFTableModelPredictModelMapper::new, params);
	}
}
