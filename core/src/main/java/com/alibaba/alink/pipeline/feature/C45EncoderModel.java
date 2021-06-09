package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.C45EncoderParams;
import com.alibaba.alink.pipeline.MapModel;

public class C45EncoderModel extends MapModel <C45EncoderModel>
	implements C45EncoderParams <C45EncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public C45EncoderModel() {this(null);}

	public C45EncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}