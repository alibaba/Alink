package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Classification model using Keras Sequential model.
 */
@NameCn("KerasSequential分类模型")
public class KerasSequentialClassificationModel
	extends TFTableModelClassificationModel <KerasSequentialClassificationModel> {

	public KerasSequentialClassificationModel() {this(null);}

	public KerasSequentialClassificationModel(Params params) {
		super(params);
	}
}
