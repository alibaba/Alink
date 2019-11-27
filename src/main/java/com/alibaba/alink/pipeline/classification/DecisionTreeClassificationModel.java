package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class DecisionTreeClassificationModel extends MapModel<DecisionTreeClassificationModel>
	implements DecisionTreePredictParams <DecisionTreeClassificationModel> {

	public DecisionTreeClassificationModel() {this(null);}

	public DecisionTreeClassificationModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
