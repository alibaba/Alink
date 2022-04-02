package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Regression model using Keras Sequential model.
 */
@NameCn("KerasSequential回归模型")
public class KerasSequentialRegressionModel
	extends TFTableModelRegressionModel <KerasSequentialRegressionModel> {

	public KerasSequentialRegressionModel() {this(null);}

	public KerasSequentialRegressionModel(Params params) {
		super(params);
	}
}
