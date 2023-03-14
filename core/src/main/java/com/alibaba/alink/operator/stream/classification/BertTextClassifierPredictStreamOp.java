package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelMapper;

/**
 * Predict with a text classifier using Bert models.
 */
@NameCn("Bert文本分类预测")
@NameEn("Bert Text Classification")
public class BertTextClassifierPredictStreamOp extends
	TFTableModelClassifierPredictStreamOp <BertTextClassifierPredictStreamOp> {

	public BertTextClassifierPredictStreamOp() {
		super();
	}

	public BertTextClassifierPredictStreamOp(Params params) {
		super(params);
	}

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
