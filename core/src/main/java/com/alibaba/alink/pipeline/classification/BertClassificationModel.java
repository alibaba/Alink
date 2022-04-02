package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Text/Text pair classification model using Bert models.
 */
@NameCn("Bert分类模型")
public class BertClassificationModel extends TFTableModelClassificationModel <BertClassificationModel> {

	public BertClassificationModel() {this(null);}

	public BertClassificationModel(Params params) {
		super(params);
	}
}
