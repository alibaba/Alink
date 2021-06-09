package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.CartEncoderParams;
import com.alibaba.alink.pipeline.MapModel;

public class CartEncoderModel extends MapModel <CartEncoderModel>
	implements CartEncoderParams <CartEncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public CartEncoderModel() {this(null);}

	public CartEncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}