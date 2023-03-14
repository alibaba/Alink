package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.bert.BertTextPairTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Text pair regressor using Bert models.
 */
@NameCn("Bert文本对回归")
public class BertTextPairRegressor extends Trainer <BertTextPairRegressor, BertRegressionModel>
	implements BertTextPairTrainParams <BertTextPairRegressor>,
	TFTableModelClassificationPredictParams <BertTextPairRegressor> {

	public BertTextPairRegressor() {this(null);}

	public BertTextPairRegressor(Params params) {
		super(params);
	}

}
