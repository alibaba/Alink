package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Ridge regression pipeline model.
 */
public class RidgeRegressionModel extends MapModel <RidgeRegressionModel>
	implements RidgeRegPredictParams <RidgeRegressionModel> {

	private static final long serialVersionUID = -5790784948452207047L;

	public RidgeRegressionModel() {this(null);}

	public RidgeRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
