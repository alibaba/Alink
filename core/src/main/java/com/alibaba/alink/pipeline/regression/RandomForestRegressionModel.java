package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class RandomForestRegressionModel extends MapModel<RandomForestRegressionModel>
	implements RandomForestRegPredictParams <RandomForestRegressionModel> {

	public RandomForestRegressionModel() {this(null);}

	public RandomForestRegressionModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
