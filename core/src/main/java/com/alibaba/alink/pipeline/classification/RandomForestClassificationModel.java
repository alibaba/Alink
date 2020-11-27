package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.RandomForestPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is random forest classification model.
 */
public class RandomForestClassificationModel extends MapModel <RandomForestClassificationModel>
	implements RandomForestPredictParams <RandomForestClassificationModel> {

	private static final long serialVersionUID = 8752997178450419817L;

	public RandomForestClassificationModel() {
		this(null);
	}

	public RandomForestClassificationModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
