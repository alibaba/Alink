package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * The model of random forest regression.
 */
public class RandomForestRegressionModel extends MapModel <RandomForestRegressionModel>
	implements RandomForestRegPredictParams <RandomForestRegressionModel> {

	private static final long serialVersionUID = -4240570290178478093L;

	public RandomForestRegressionModel() {this(null);}

	public RandomForestRegressionModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
