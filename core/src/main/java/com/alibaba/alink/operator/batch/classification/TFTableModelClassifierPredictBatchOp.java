package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.FlatModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationFlatModelMapper;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;

@Internal
@NameCn("TF表模型分类预测")
public class TFTableModelClassifierPredictBatchOp<T extends TFTableModelClassifierPredictBatchOp <T>>
	extends FlatModelMapBatchOp <T> implements TFTableModelClassificationPredictParams <T> {

	public TFTableModelClassifierPredictBatchOp() {
		this(new Params());
	}

	public TFTableModelClassifierPredictBatchOp(Params params) {
		super(TFTableModelClassificationFlatModelMapper::new, params);
	}
}
