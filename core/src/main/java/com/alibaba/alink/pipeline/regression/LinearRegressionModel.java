package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Linear regression pipeline model.
 *
 */
public class LinearRegressionModel extends MapModel<LinearRegressionModel> {

	public LinearRegressionModel() {this(null);}

	public LinearRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
