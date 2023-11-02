package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.TargetEncoderModelMapper;
import com.alibaba.alink.params.feature.TargetEncoderPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("Target Encoder Model")
@NameEn("Target Encoder Model")
public class TargetEncoderModel extends MapModel <TargetEncoderModel>
	implements TargetEncoderPredictParams <TargetEncoderModel> {

	public TargetEncoderModel() {
		this(new Params());
	}

	public TargetEncoderModel(Params params) {
		super(TargetEncoderModelMapper::new, params);
	}
}
