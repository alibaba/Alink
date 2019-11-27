package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Lasso regression pipeline model.
 *
 */
public class LassoRegressionModel extends MapModel<LassoRegressionModel> {

	public LassoRegressionModel() {this(null);}

	public LassoRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
