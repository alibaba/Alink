package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.Id3EncoderParams;
import com.alibaba.alink.pipeline.MapModel;

public class Id3EncoderModel extends MapModel <Id3EncoderModel>
	implements Id3EncoderParams <Id3EncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public Id3EncoderModel() {this(null);}

	public Id3EncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}