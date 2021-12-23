package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Classification model using Keras Sequential model.
 */
public class KerasSequentialClassificationModel
	extends TFTableModelClassificationModel <KerasSequentialClassificationModel> {

	public KerasSequentialClassificationModel() {this(null);}

	public KerasSequentialClassificationModel(Params params) {
		super(params);
	}
}
