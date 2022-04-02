package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * The model of decision tree classification.
 */
@NameCn("决策树分类器模型")
public class DecisionTreeClassificationModel extends MapModel <DecisionTreeClassificationModel>
	implements DecisionTreePredictParams <DecisionTreeClassificationModel> {

	private static final long serialVersionUID = -7770023408500542175L;

	public DecisionTreeClassificationModel() {this(null);}

	public DecisionTreeClassificationModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
