package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.CartRegEncoderParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("Cart回归编码模型")
public class CartRegEncoderModel extends MapModel <CartRegEncoderModel>
	implements CartRegEncoderParams <CartRegEncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public CartRegEncoderModel() {this(null);}

	public CartRegEncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}
