package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.bert.BertTextTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Text classifier using Bert models.
 */
@NameCn("Bert文本分类器")
public class BertTextClassifier extends Trainer <BertTextClassifier, BertClassificationModel>
	implements BertTextTrainParams <BertTextClassifier>,
	TFTableModelClassificationPredictParams <BertTextClassifier> {

	public BertTextClassifier() {this(null);}

	public BertTextClassifier(Params params) {
		super(params);
	}

}
