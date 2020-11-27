package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * The model of decision tree regression.
 */
public class DecisionTreeRegressionModel extends MapModel <DecisionTreeRegressionModel>
	implements DecisionTreeRegPredictParams <DecisionTreeRegressionModel> {

	private static final long serialVersionUID = 2247135234053984326L;

	public DecisionTreeRegressionModel() {this(null);}

	public DecisionTreeRegressionModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
