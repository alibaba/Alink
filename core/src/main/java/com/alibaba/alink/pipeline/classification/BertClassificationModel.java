package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

public class BertClassificationModel extends TFTableModelClassificationModel <BertClassificationModel> {

	public BertClassificationModel() {this(null);}

	public BertClassificationModel(Params params) {
		super(params);
	}
}
