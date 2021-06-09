package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.GbdtRegEncoderParams;
import com.alibaba.alink.pipeline.MapModel;

public class GbdtRegEncoderModel extends MapModel <GbdtRegEncoderModel>
	implements GbdtRegEncoderParams <GbdtRegEncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public GbdtRegEncoderModel() {this(null);}

	public GbdtRegEncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}