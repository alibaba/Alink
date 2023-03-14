package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictFlatModelMapper;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;

/**
 * This operator loads a tensorflow SavedModel model wrapped in Alink Model format, and performs prediction with
 * Tensorflow's Java sdk.
 */
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("TF表模型预测")
@NameEn("TF TableModel Prediction")
public final class TFTableModelPredictBatchOp extends FlatModelMapBatchOp <TFTableModelPredictBatchOp>
	implements TFTableModelPredictParams <TFTableModelPredictBatchOp> {

	public TFTableModelPredictBatchOp() {
		this(new Params());
	}

	public TFTableModelPredictBatchOp(Params params) {
		super(TFTableModelPredictFlatModelMapper::new, params);
	}
}
