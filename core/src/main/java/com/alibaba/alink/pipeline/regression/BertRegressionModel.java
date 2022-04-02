package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Text/Text pair regression model using Bert models.
 */
@NameCn("Bert回归模型")
public class BertRegressionModel extends TFTableModelRegressionModel <BertRegressionModel> {

	public BertRegressionModel() {this(null);}

	public BertRegressionModel(Params params) {
		super(params);
	}
}
