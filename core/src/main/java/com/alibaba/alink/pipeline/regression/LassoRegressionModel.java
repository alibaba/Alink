package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LassoRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Lasso regression pipeline model.
 */
public class LassoRegressionModel extends MapModel <LassoRegressionModel>
	implements LassoRegPredictParams <LassoRegressionModel> {

	private static final long serialVersionUID = 6635150109381708196L;

	public LassoRegressionModel() {this(null);}

	public LassoRegressionModel(Params params) {
		super(LinearModelMapper::new, params);
	}

}
