package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;

/**
 * Prediction with text pair classifier using Bert models.
 */
@NameCn("Bert文本对分类预测")
@NameEn("Bert Text Pair Classifier Prediction")
public class BertTextPairClassifierPredictBatchOp extends TFTableModelClassifierPredictBatchOp <BertTextPairClassifierPredictBatchOp> {

	public BertTextPairClassifierPredictBatchOp() {
		this(new Params());
	}

	public BertTextPairClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
