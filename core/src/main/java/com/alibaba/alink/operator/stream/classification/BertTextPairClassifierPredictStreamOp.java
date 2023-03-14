package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Predict with a text pair classifier using Bert models.
 */
@NameCn("Bert文本对分类预测")
@NameEn("Bert Text Pair Classification")
public class BertTextPairClassifierPredictStreamOp extends
	TFTableModelClassifierPredictStreamOp <BertTextPairClassifierPredictStreamOp> {

	public BertTextPairClassifierPredictStreamOp() {
		super();
	}

	public BertTextPairClassifierPredictStreamOp(Params params) {
		super(params);
	}

	public BertTextPairClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextPairClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
