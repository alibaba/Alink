package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Ridge regression pipeline model.
 *
 */
public class RidgeRegressionModel extends MapModel<RidgeRegressionModel> {

	public RidgeRegressionModel() {this(null);}

	public RidgeRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
