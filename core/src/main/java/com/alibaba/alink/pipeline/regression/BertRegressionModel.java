package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

public class BertRegressionModel extends TFTableModelRegressionModel <BertRegressionModel> {

	public BertRegressionModel() {this(null);}

	public BertRegressionModel(Params params) {
		super(params);
	}
}
