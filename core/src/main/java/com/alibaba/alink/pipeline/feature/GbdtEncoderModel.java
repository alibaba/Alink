package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.GbdtEncoderParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("GBDT编码模型")
public class GbdtEncoderModel extends MapModel <GbdtEncoderModel>
	implements GbdtEncoderParams <GbdtEncoderModel> {

	private static final long serialVersionUID = -44913012987710695L;

	public GbdtEncoderModel() {this(null);}

	public GbdtEncoderModel(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

}
