package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.bert.BertTextPairTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Text pair classifier using Bert models.
 */
@NameCn("Bert文本对分类器")
public class BertTextPairClassifier extends Trainer <BertTextPairClassifier, BertClassificationModel>
	implements BertTextPairTrainParams <BertTextPairClassifier>,
	TFTableModelClassificationPredictParams <BertTextPairClassifier> {

	public BertTextPairClassifier() {this(null);}

	public BertTextPairClassifier(Params params) {
		super(params);
	}

}
