package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

public class KerasSequentialClassificationModel
	extends TFTableModelClassificationModel <KerasSequentialClassificationModel> {

	public KerasSequentialClassificationModel() {this(null);}

	public KerasSequentialClassificationModel(Params params) {
		super(params);
	}
}
