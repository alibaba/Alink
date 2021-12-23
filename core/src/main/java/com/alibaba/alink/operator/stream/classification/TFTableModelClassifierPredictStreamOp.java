package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;

@Internal
public class TFTableModelClassifierPredictStreamOp<T extends TFTableModelClassifierPredictStreamOp <T>>
	extends ModelMapStreamOp <T> implements TFTableModelClassificationPredictParams <T> {

	public TFTableModelClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public TFTableModelClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, TFTableModelClassificationModelMapper::new, params);
	}
}
