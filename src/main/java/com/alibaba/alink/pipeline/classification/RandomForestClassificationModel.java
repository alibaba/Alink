package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.RandomForestPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class RandomForestClassificationModel extends MapModel<RandomForestClassificationModel>
	implements RandomForestPredictParams <RandomForestClassificationModel> {

	public RandomForestClassificationModel() {this(null);}

	public RandomForestClassificationModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
