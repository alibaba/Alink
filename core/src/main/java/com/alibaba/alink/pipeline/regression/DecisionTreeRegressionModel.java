package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class DecisionTreeRegressionModel extends MapModel<DecisionTreeRegressionModel>
	implements DecisionTreeRegPredictParams <DecisionTreeRegressionModel> {

	public DecisionTreeRegressionModel() {this(null);}

	public DecisionTreeRegressionModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
