package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.DecisionTreeRegEncoderParams;
import com.alibaba.alink.pipeline.MapModel;

public class DecisionTreeRegEncoderModel extends MapModel <DecisionTreeRegEncoderModel>
	implements DecisionTreeRegEncoderParams <DecisionTreeRegEncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public DecisionTreeRegEncoderModel() {this(null);}

	public DecisionTreeRegEncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}