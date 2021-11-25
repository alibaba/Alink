package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Regression model using Keras Sequential model.
 */
public class KerasSequentialRegressionModel
	extends TFTableModelRegressionModel <KerasSequentialRegressionModel> {

	public KerasSequentialRegressionModel() {this(null);}

	public KerasSequentialRegressionModel(Params params) {
		super(params);
	}
}
