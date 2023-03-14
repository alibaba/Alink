package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.feature.TargetEncoderPredictParams;
import com.alibaba.alink.params.feature.TargetEncoderTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class TargetEncoder extends Trainer <TargetEncoder, TargetEncoderModel>
	implements TargetEncoderTrainParams <TargetEncoder>,
	TargetEncoderPredictParams <TargetEncoder> {

	public TargetEncoder() {
		this(new Params());
	}

	public TargetEncoder(Params params) {
		super(params);
	}

}
